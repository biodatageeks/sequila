package org.biodatageeks.sequila.rangejoins.exp.iitii

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object ImplicitTreeAugmenter {

  def augmentInsideMax[V](implicitIntervalTree: ArrayBuffer[AugmentedNode[V]], K: Int): Unit = {
    val rootRank = math.pow(2, K).toInt - 1
    val highestRank = implicitIntervalTree.size - 1
    getMaxSubtree(implicitIntervalTree, rootRank, K, highestRank)
  }

  def augmentOutsideMax[V](implicitIntervalTree: ArrayBuffer[AugmentedNode[V]], K: Int): Unit = {
    val runningMaxEnd: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    runningMaxEnd.append(implicitIntervalTree(0).getEnd)
    for (i <- 1 until implicitIntervalTree.length) {
      runningMaxEnd.append(math.max(runningMaxEnd(i - 1), implicitIntervalTree(i).getEnd))
    }

    for (i <- implicitIntervalTree.indices) {
      val node = implicitIntervalTree(i)
      val l = leftMostLeaf(i)

      if (l > 0) {
        var leq = l - 1
        breakable {
          while (implicitIntervalTree(leq).getStart == node.getStart) {
            if (leq == 0) {
              break()
            }
            leq -= 1
          }
        }
        if (implicitIntervalTree(leq).getStart <= node.getStart) {
          node.outsideMaxEnd = runningMaxEnd(leq)
        } else {
          node.outsideMaxEnd = 0
        }
      }
    }
  }

  private def getMaxSubtree[V](tree: ArrayBuffer[AugmentedNode[V]], rank: Int, level: Int, highestRank: Int): Int = {
    if (level == 0 && rank <= highestRank) {
      // regular leaf
      tree(rank).insideMaxEnd = tree(rank).getEnd
      return tree(rank).insideMaxEnd
    } else if (level == 0 && rank > highestRank) {
      // imaginary leaf
      return -1
    } else if (level > 0 && rank <= highestRank) {
      // only right can be imaginary here but can return some value
      val leftSubtreeMax = getMaxSubtree(tree, rank - scala.math.pow(2, level - 1).toInt, level - 1, highestRank)
      val rightSubtreeMax = getMaxSubtree(tree, rank + scala.math.pow(2, level - 1).toInt, level - 1, highestRank)
      tree(rank).insideMaxEnd = scala.math.max(scala.math.max(leftSubtreeMax, rightSubtreeMax), tree(rank).getEnd)
      return tree(rank).insideMaxEnd
    } else if (level > 0 && rank > highestRank) {
      // imaginary node, it makes sens to analyze only left subtree cos right is always imaginary
      return getMaxSubtree(tree, rank - scala.math.pow(2, level - 1).toInt, level - 1, highestRank)
    }
    throw new FixImplementationException
  }

  private def leftMostLeaf(index: Int, level: Int): Int = {
    val ofs = (1 << level) - 1;
    index - ofs;
  }

  private def leftMostLeaf(index: Int): Int = {
    leftMostLeaf(index, LevelResolver.levelForIndex(index))
  }
}
