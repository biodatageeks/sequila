package org.biodatageeks.sequila.rangejoins.exp.iitii

import org.biodatageeks.sequila.rangejoins.methods.base.{BaseIntervalHolder, BaseNode}

import java.util
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

class ImplicitIntervalTreeWithInterpolationIndex[V] extends BaseIntervalHolder[V] {

  var intervals: ArrayBuffer[AugmentedNode[V]] = ArrayBuffer()
  var internalTree: ArrayBuffer[AugmentedNode[V]] = _
  var arraySizeIncludingImaginaryNodes: Int = -1
  var K: Int = -1
  var domainSize: Int = -1
  var minStart: Int = -1
  val NUMBER_OF_DOMAINS = 4096
  val parameters: ArrayBuffer[(Double, Double, Double)] = new ArrayBuffer[(Double, Double, Double)](NUMBER_OF_DOMAINS)
  val invalidIndex: Int = Integer.MAX_VALUE
  var queries: Int = 0
  var totalClimbCost = 0
  var rootIndex: Int = -1

  override def put(start: Int, end: Int, value: V): V = {
    intervals += new AugmentedNode[V](start, end, value)
    value
  }

  override def postConstruct: Unit = {
    val dummyValue = 0.0
    0 until NUMBER_OF_DOMAINS foreach { _ =>
      parameters.append((dummyValue, dummyValue, dummyValue))
    }
    internalTree = createImplicitIntervalTree(intervals)
    val result: TreeMetadata = TreeMetadataRetriever.resolveKAndArraySizeIncludingImaginaryNodes(internalTree.size)
    println("Tree size = ".concat(result.K.toString))
    K = result.K
    arraySizeIncludingImaginaryNodes = result.arraySizeIncludingImaginaryNodes
    minStart = internalTree(0).getStart
    rootIndex = (1 << K) - 1
    domainSize = 1 + (internalTree(internalTree.size - 1).getStart - minStart) / NUMBER_OF_DOMAINS
    ImplicitTreeAugmenter.augmentInsideMax(internalTree, K)
    ImplicitTreeAugmenter.augmentOutsideMax(internalTree, K)
    trainModel(internalTree)
  }

  def interpolate(level: Int, w0: Double, w1: Double, start: Int): Int = {
    val ofs = w0 + w1 * start.toFloat
    val r: Int = IndexLevelResolver.indexForIndexLevelAndLevel(math.max(0.0, math.round(ofs)).toInt, level)
    // if rank is imaginary (start is off-scale high), start from rightmost real leaf
    val nsz = this.internalTree.size
    if (r < nsz) {
      r
    } else {
      nsz - (2 - nsz % 2)
    }
  }

  private def trainModel(internalTree: ArrayBuffer[AugmentedNode[V]]): Unit = {
    // Fibonacci-ish series of tree levels at which to evaluate interpolation model fit
    val trainLevels: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    trainLevels.append(0, 1, 2, 4, 7, 12, 20, 33, 54)
    val partitionedList: ArrayBuffer[ArrayBuffer[ModelTuple]] = new ArrayBuffer[ArrayBuffer[ModelTuple]](NUMBER_OF_DOMAINS)
    0 until NUMBER_OF_DOMAINS foreach { _ =>
      partitionedList.append(new ArrayBuffer[ModelTuple]())
    }
    for (i <- internalTree.indices) {
      partitionedList(whichDomain(internalTree(i).getStart)).append(ModelTuple(internalTree(i).getStart, i))
    }
    for (domain <- 0 until NUMBER_OF_DOMAINS) {
      val pointsByLevel: ArrayBuffer[ArrayBuffer[ModelLevelTuple]] = new ArrayBuffer[ArrayBuffer[ModelLevelTuple]](K + 1)
      0 until (K + 1) foreach { _ =>
        pointsByLevel.append(new ArrayBuffer[ModelLevelTuple]())
      }
      for (point <- partitionedList(domain)) {
        val level = computeLevel(point.index)
        pointsByLevel(level).append(ModelLevelTuple(point.start, IndexLevelResolver.levelIndexForIndex(point.index)))
      }
      var lowestCost: Double = Double.MaxValue
      breakable {
        for (i <- trainLevels.indices) {
          val trainLevel = trainLevels(i)
          if (trainLevel >= K || pointsByLevel(trainLevel).size <= 1) {
            break()
          }
          // regress points on this level
          val w: RegressionResult = regress(pointsByLevel(trainLevel))

          if (w.slope != 0.0) {
            // calculate estimate of search cost (average over all domain points)
            var cost: Int = 0
            partitionedList(domain).foreach(p => {
              val xStart = p.start
              val yIndex = p.index
              val fx = interpolate(trainLevel, w.intercept, w.slope, xStart)
              val error: Int = if (fx >= yIndex) {
                (fx - yIndex) / 1 << trainLevel
              } else {
                (yIndex - fx) / 1 << trainLevel
              }
              val errorPenalty: Int = if (error != 0) {
                2 * (1 + log2ull(error))
              } else {
                0
              }
              val overlapPenalty = this.internalTree(fx).outsideMaxEnd
              cost = cost + trainLevel + math.max(errorPenalty, overlapPenalty)
            })
            val avgCost: Double = cost.toDouble / partitionedList(domain).size

            if (avgCost < K && avgCost < lowestCost) {
              lowestCost = avgCost
              parameters(domain) = (w.intercept, w.slope, trainLevel)
            }
          }
        }
      }
      // free memory
      partitionedList(domain).clear()
    }
  }

  private val log2 = (x: Double) => math.log10(x) / math.log10(2.0)

  private def log2ull(x: Long): Int = {
    math.floor(log2(x)).toInt
  }

  private def regress(points: ArrayBuffer[ModelLevelTuple]): RegressionResult = {
    if (points.size <= 1) {
      return RegressionResult(0, 0)
    }
    var sumX, sumY, cov, variance: Double = 0.0
    points.foreach(p => {
      sumX = sumX + p.start
      sumY = sumY + p.levelIndex
    })
    val meanX = sumX / points.size.toDouble
    val meanY = sumY / points.size.toDouble
    points.foreach(p => {
      val xErr: Double = p.start - meanX
      cov += xErr * (p.levelIndex - meanY)
      variance += xErr * xErr
    })

    if (variance == 0.0) {
      return RegressionResult(0, 0)
    }
    val m: Double = cov / variance
    RegressionResult(meanY - m * meanX, m)
  }

  private def computeLevel(index: Int): Int = {
    LevelResolver.levelForIndex(index)
  }

  private def whichDomain(start: Int): Int = {
    if (start < this.internalTree(0).getStart) {
      return 0
    }
    math.min(NUMBER_OF_DOMAINS - 1, (start - minStart) / domainSize)
  }

  private def createImplicitIntervalTree(intervals: ArrayBuffer[AugmentedNode[V]]): ArrayBuffer[AugmentedNode[V]] = {
    intervals.sortBy(i => (i.getStart, i.getEnd))
  }

  override def remove(start: Int, end: Int): V = throw new RuntimeException

  override def find(start: Int, end: Int): BaseNode[V] = throw new RuntimeException

  override def overlappers(qStart: Int, qEnd: Int): util.Iterator[BaseNode[V]] = {
    val results = new ArrayBuffer[BaseNode[V]]()
    // ask model which leaf we should begin our bottom-up climb at
    val prediction: Int = predict(qStart)
    if (prediction == invalidIndex) {
      return simpleOverlap(qStart, qEnd, results)
    }

    val k0 = LevelResolver.levelForIndex(prediction)
    // climb until our necessary & sufficient criteria are met, or the root
    var subtree = prediction
    var k = k0
    // stop at root
    // continue climb through imaginary
    // possible outside overlap from left
    while (subtree != rootIndex && (subtree >= internalTree.size || qStart <= this.internalTree(subtree).outsideMaxEnd || outsideMinStart(subtree, k) <= qEnd)) {
      subtree = parent(subtree, k)
      k += 1
    }

    val climbCost: Int = k - k0;
    queries = queries + 1
    totalClimbCost += climbCost
    scan(subtree, k, qStart, qEnd, results)
    results.iterator
  }

  def left(index: Int, level: Int): Int = {
    if (level > 0) {
      index - (1 << (level - 1))
    } else {
      invalidIndex
    }
  }

  // get node's right child, or invalidIndex if called on a leaf
  def right(index: Int, level: Int): Int = {
    if (level > 0) {
      index + (1 << (level - 1))
    } else {
      invalidIndex
    }
  }

  def leftMostLeaf(index: Int, level: Int): Int = {
    val ofs = (1 << level) - 1;
    index - ofs;
  }

  def leftMostLeaf(index: Int): Int = {
    leftMostLeaf(index, LevelResolver.levelForIndex(index))
  }

  def rightMostLeaf(index: Int, level: Int): Int = {
    val ofs = (1 << level) - 1;
    index + ofs
  }

  def rightMostLeaf(index: Int): Int = {
    rightMostLeaf(index, LevelResolver.levelForIndex(index))
  }

  private def scan(index: Int, level: Int, qStart: Int, qEnd: Int, results: ArrayBuffer[BaseNode[V]]): Unit = {
    // When we arrive at an imaginary node, its right subtree must be all imaginary, so we
    // only need to descend left.
    if (index >= this.internalTree.size) {
      if (level > 0) {
        scan(left(index, level), level - 1, qStart, qEnd, results)
      }
      return
    } else if (level <= 2) {
      val leftMostLeafIndex = leftMostLeaf(index, level)
      val rightMostLeafIndex = math.min(rightMostLeaf(index, level), this.internalTree.size - 1)
      var rIndex = leftMostLeafIndex
      breakable {
        while (rIndex <= rightMostLeafIndex) {
          if (this.internalTree(rIndex).getStart > qEnd) {
            break()
          }
          if (this.internalTree(rIndex).getEnd >= qStart) {
            results.append(this.internalTree(rIndex))
          }
          rIndex += 1
        }
      }
      return
    }
    val node = this.internalTree(index)
    if (node.insideMaxEnd >= qStart) {
      val ck: Int = level - 1
      scan(left(index, level), ck, qStart, qEnd, results)
      val nStart = this.internalTree(index).getStart
      if (nStart <= qEnd) { // this node isn't already past query
        if (this.internalTree(index).getEnd >= qStart) { // this node overlaps query
          results.append(this.internalTree(index))
        }
        scan(right(index, level), ck, qStart, qEnd, results);
      }
    }
  }

  // get node's parent, or invalidIndex if called on the root
  private def parent(index: Int, level: Int): Int = {
    val ofs: Int = 1 << level
    if (((index >> (level + 1)) & 1) != 0) { // node is right child
      return index - ofs
    }
    // node is a left child
    index + ofs
  }

  private def parent2(index: Int): Int = {
    // helper to compute level if it isn't already known (which it often is)
    parent(index, LevelResolver.levelForIndex(index));
  }

  private def outsideMinStart(index: Int, level: Int): Int = {
    val r: Int = rightMostLeaf(index, level)
    val start: Int = this.internalTree(index).getStart
    val l = leftMostLeaf(index, level)
    if (l > 0 && this.internalTree(l - 1).getStart == start) {
      // corner case: nodes to the left of the subtree can have the same beg as subroot
      // and outside_min_start is defined on nodes with start >= subroot's.
      start
    } else {
      if (r < this.internalTree.size - 1) {
        this.internalTree(r + 1).getStart
      } else {
        Integer.MAX_VALUE
      }
    }
  }

  override def iterator(): util.Iterator[BaseNode[V]] = throw new RuntimeException

  private def predict(start: Int): Int = {
    val which = whichDomain(start)
    val model: (Double, Double, Double) = parameters(which)
    val lv_f: Double = model._3
    if (lv_f < 0 || model._2 == 0.0) {
      return invalidIndex
    }
    val k = lv_f.toInt
    interpolate(k, model._1, model._2, start)
  }

  private def simpleOverlap(start: Int, end: Int, results: ArrayBuffer[BaseNode[V]]): util.Iterator[BaseNode[V]] = {
    scan(rootIndex, K, start, end, results)
    results.iterator
  }
}

case class ModelTuple(start: Int, index: Int)

case class ModelLevelTuple(start: Int, levelIndex: Int)

case class RegressionResult(intercept: Double, slope: Double)





