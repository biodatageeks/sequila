package org.biodatageeks.sequila.pileup.model


import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/** Events aggregation on contig
  */
case class ContigEventAggregate (
                                  contig: String = "",
                                  contigLen: Int = 0,
                                  events: Array[Short]= Array(0),
                                  startPosition: Int = 0,
                                  maxPosition: Int = 0,
                                  maxSeqLen:Int = 0
                                )


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
                     cumSum: Short
                   )

/**
  * holds ranges of contig ranges (in particular contigAggregate)
  * @param contig
  * @param minPos
  * @param maxPos
  */
case class ContigRange(
                        contig: String,
                        minPos: Int,
                        maxPos: Int
                      )


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
                         upd: mutable.HashMap[(String,Int), (Option[Array[Short]],Short)],
                         shrink: mutable.HashMap[(String,Int), Int],
                         minmax: mutable.HashMap[String,(Int,Int)]
                       )

