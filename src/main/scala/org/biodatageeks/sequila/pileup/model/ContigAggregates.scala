package org.biodatageeks.sequila.pileup.model


import org.apache.spark.util.AccumulatorV2
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/** Events aggregation on contig
  */
case class ContigEventAggregate (
                                  contig: String = "",
                                  contigLen: Int = 0,
                                  events: Array[Short],
                                  alts: mutable.HashMap[Int, mutable.HashMap[Byte,Short]], // todo make altsMap class
                                  startPosition: Int = 0,
                                  maxPosition: Int = 0,
                                  shrinkedEventsArraySize: Int = 0,
                                  maxSeqLen:Int = 0
                                ) {
 def hasAltOnPosition(pos:Int):Boolean = alts.contains(pos)

  def calculateMaxLength(allPositions: Boolean): Int = {
    if (! allPositions)
      return events.length

    val firstBlockMaxLength = startPosition - 1
    val lastBlockMaxLength = contigLen - maxPosition
    firstBlockMaxLength + events.length + lastBlockMaxLength

  }

  /**
    * updates events array for contig. Should be invoked with delta = 1 for alignment start and -1 for alignment stop
    *
    * @param pos            - position to be changed
    * @param startPart      - starting position of partition (offset)
    * @param delta          - value to be added to position
    */

  def updateEvents(pos: Int, startPart: Int, delta: Short): Unit = {
    val position = pos - startPart
    events(position) = (events(position) + delta).toShort
  }

  def updateAlts(pos: Int, startPart: Int, alt: Char): Unit = {
   // val position = pos - startPart - 1
    val position = pos // naturally indexed
    val altByte = alt.toByte

    if (alts.contains(position)) {
      val altMap = alts(position)
      if (altMap.contains(altByte)) {
        val prevAltCount = altMap(altByte)
        alts(position)(altByte) = (prevAltCount + 1).toShort
      }
      else
        alts(position) += (altByte -> 1)
    }
    else
      alts(position) = mutable.HashMap(altByte -> 1)

    assert(alts(position) != null)
  }
}


/**
  * contains only tails of contigAggregates
  * @param contig
  * @param minPos
  * @param startPoint
  * @param events
  * @param cumSum
  */
case class TailEdge(
                     contig: String,
                     minPos: Int,
                     startPoint: Int,
                     events: Array[Short],
                     alts: mutable.HashMap[Int, mutable.HashMap[Byte,Short]],
                     cumSum: Short
                   )

/**
  * holds ranges of contig ranges (in particular contigAggregate)
  * @param contig
  * @param minPos
  * @param maxPos
  */
case class ContigRange(contig: String, minPos: Int,maxPos: Int) {

  def findOverlappingTails(tails: ArrayBuffer[TailEdge]):ArrayBuffer[TailEdge] = {
    tails
      .filter(t => (t.contig == contig && t.startPoint + t.events.length > minPos) && t.minPos < minPos)
      .sortBy(r => (r.contig, r.minPos))
  }

  def precedingCumulativeSum(tails: ArrayBuffer[TailEdge]): Short = {
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
                    var tails: ArrayBuffer[TailEdge],
                    var ranges: ArrayBuffer[ContigRange]
                  ) extends Serializable {

  def reset(): Unit = {
    tails = new ArrayBuffer[TailEdge]()
    ranges = new ArrayBuffer[ContigRange]()
  }

  def add(p: PileupUpdate): PileupUpdate = {
    tails = tails ++ p.tails
    ranges = ranges ++ p.ranges
    this
  }

}

/**
  * accumulator gathering potential overlaps between contig aggregates in consecutive partitions
  * @param pilAcc update structure with tail info and contig range info
  */
class PileupAccumulator(var pilAcc: PileupUpdate)
  extends AccumulatorV2[PileupUpdate, PileupUpdate] {

  def reset(): Unit = {
    pilAcc = new PileupUpdate(new ArrayBuffer[TailEdge](),
      new ArrayBuffer[ContigRange]())
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
                         upd: mutable.HashMap[(String,Int), (Option[Array[Short]], Option[mutable.HashMap[Int, mutable.HashMap[Byte,Short]]], Short)],
                         shrink: mutable.HashMap[(String,Int), Int],
                         minmax: mutable.HashMap[String,(Int,Int)]
                       )

