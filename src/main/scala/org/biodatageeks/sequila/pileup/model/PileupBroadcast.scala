package org.biodatageeks.sequila.pileup.model

import org.apache.spark.util.AccumulatorV2
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * contains only tails of contigAggregates
  *
  * @param contig
  * @param minPos
  * @param startPoint
  * @param events
  * @param cumSum
  */
case class Tail(
                     contig: String,
                     minPos: Int,
                     startPoint: Int,
                     events: Array[Short],
                     alts: MultiLociAlts,
                     cumSum: Short
                   )

/**
  * holds ranges of contig ranges (in particular contigAggregate)
  * @param contig
  * @param minPos
  * @param maxPos
  */
case class Range(contig: String, minPos: Int, maxPos: Int) {

  def findOverlappingTails(tails: ArrayBuffer[Tail]):ArrayBuffer[Tail] = {
    tails
      .filter(t => (t.contig == contig && t.startPoint + t.events.length > minPos) && t.minPos < minPos)
      .sortBy(r => (r.contig, r.minPos))
  }

  def precedingCumulativeSum(tails: ArrayBuffer[Tail]): Short = {
    val tailsFiltered = tails
      .filter(t => t.contig == contig && t.minPos < minPos)
    val cumSum = tailsFiltered.map(_.cumSum)
    val cumsSumArr = cumSum.toArray
    FastMath.sumShort(cumsSumArr)
  }
}


/**
  * keeps update structure
  * @param tails array of tails of contigAggregates
  * @param ranges array of contig ranges
  */
class PileupUpdate(
                    var tails: ArrayBuffer[Tail],
                    var ranges: ArrayBuffer[Range]
                  ) extends Serializable {

  def reset(): Unit = {
    tails = new ArrayBuffer[Tail]()
    ranges = new ArrayBuffer[Range]()
  }

  def add(p: PileupUpdate): PileupUpdate = {
    tails = tails ++ p.tails
    ranges = ranges ++ p.ranges
    this
  }

  /**
    * calculate actual overlaps between slices. They will be then broadcasted.
    * @return - structure for broadcast
    */
  def prepareOverlaps(): UpdateStruct = {
    val updateMap = new mutable.HashMap[(String, Int), (Option[Array[Short]], Option[MultiLociAlts], Short)]()
    val shrinkMap = new mutable.HashMap[(String, Int), Int]()

    var it = 0
    for (range <- ranges.sortBy(r => (r.contig, r.minPos))) {

      val overlaps = range.findOverlappingTails(tails)
      val cumSum = range.precedingCumulativeSum(tails)

      if(overlaps.isEmpty)
        updateMap += (range.contig, range.minPos) -> (None, None, cumSum)
      else { // if there are  overlaps for this contigRange
        for(o <- overlaps) {
          val overlapLength = calculateOverlapLength(o, range, it, ranges)
          updateShrinksByOverlap(o, range, shrinkMap)
          updateUpdateByOverlap(o, overlapLength, range, cumSum, updateMap)
        }
      }
      it += 1
    }
    UpdateStruct(updateMap, shrinkMap)
  }


  private def updateUpdateByOverlap(o: Tail, overlapLength: Int, range: Range, cumSum: Short, updateMap: mutable.HashMap[(String, Int), (Option[Array[Short]], Option[MultiLociAlts], Short)]) = {
    updateMap.get((range.contig, range.minPos)) match {
      case Some(up) =>
        val arrEvents = Array.fill[Short](math.max(0, o.startPoint - range.minPos))(0) ++ o.events.takeRight(overlapLength)
        val newArrEvents = up._1.get.zipAll(arrEvents, 0.toShort, 0.toShort).map { case (x, y) => (x + y).toShort }

        val newAlts = (up._2.get ++ o.alts).asInstanceOf[MultiLociAlts]

        val newCumSum = (up._3 - FastMath.sumShort(o.events.takeRight(overlapLength)) ).toShort

        if (o.minPos < range.minPos)
          updateMap.update((range.contig, range.minPos), (Some(newArrEvents), Some(newAlts), newCumSum))
        else
          updateMap.update((range.contig, o.minPos), (Some(newArrEvents), Some(newAlts), newCumSum)) // delete anything that is > range.minPos
      case _ =>
        updateMap +=
          (range.contig, range.minPos) ->
            (
              Some(Array.fill[Short](math.max(0, o.startPoint - range.minPos))(0) ++ o.events.takeRight(overlapLength)),
              Some(o.alts),
              (cumSum - FastMath.sumShort(o.events.takeRight(overlapLength)) ).toShort
            )
    }
  }

  @inline private def updateShrinksByOverlap(o: Tail, range: Range, shrinkMap: mutable.HashMap[(String, Int), Int]) = {
    shrinkMap.get((o.contig, o.minPos)) match {
      case Some(s) => shrinkMap.update((o.contig, o.minPos), math.min(s, range.minPos - o.minPos + 1))
      case _ => shrinkMap += (o.contig, o.minPos) -> (range.minPos - o.minPos + 1)
    }
  }

  @inline private def calculateOverlapLength(o: Tail, range: Range, it: Int, ranges: ArrayBuffer[Range]) = {
    val length = if ((o.startPoint + o.events.length) > range.maxPos && ((ranges.length - 1 == it) || ranges(it + 1).contig != range.contig))
      o.startPoint + o.events.length - range.minPos + 1
    else if ((o.startPoint + o.events.length) > range.maxPos)
      range.maxPos - range.minPos
    //if it's the last part in contig or the last at all
    else
      o.startPoint + o.events.length - range.minPos + 1
    length
  }


}

/**
  * accumulator gathering potential overlaps between contig aggregates in consecutive partitions
  * @param pilAcc update structure with tail info and contig range info
  */
class PileupAccumulator(var pilAcc: PileupUpdate)
  extends AccumulatorV2[PileupUpdate, PileupUpdate] {

  def reset(): Unit = {
    pilAcc = new PileupUpdate(new ArrayBuffer[Tail](),
      new ArrayBuffer[Range]())
  }

  def add(v: PileupUpdate): Unit = {
    pilAcc.add(v)
  }
  def value(): PileupUpdate = {
    pilAcc
  }
  def isZero(): Boolean = {
    pilAcc.tails.isEmpty && pilAcc.ranges.isEmpty
  }
  def copy(): PileupAccumulator = {
    new PileupAccumulator(pilAcc)
  }
  def merge(other: AccumulatorV2[PileupUpdate, PileupUpdate]): Unit = {
    pilAcc.add(other.value)
  }
}

case class UpdateStruct(
                         upd: mutable.HashMap[(String,Int), (Option[Array[Short]], Option[MultiLociAlts], Short)],
                         shrink: mutable.HashMap[(String,Int), Int]
                       )

