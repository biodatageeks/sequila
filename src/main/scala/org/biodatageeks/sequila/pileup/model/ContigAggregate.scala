package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.pileup.timers.PileupTimers.{TailAltsTimer, TailCovTimer, TailEdgeTimer}
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/** Events aggregation on contig
  */
case class ContigAggregate(
                            contig: String = "",
                            contigLen: Int = 0,
                            events: Array[Short],
                            alts: MultiLociAlts,
                            startPosition: Int = 0,
                            maxPosition: Int = 0,
                            shrinkedEventsArraySize: Int = 0,
                            maxSeqLen:Int = 0
                                ) {

  def hasAltOnPosition(pos:Int):Boolean = alts.contains(pos)
  def getRange: Range = Range(contig, startPosition, maxPosition)
  def getPileupUpdate:PileupUpdate = new PileupUpdate(ArrayBuffer(getTail), ArrayBuffer(getRange))


  def calculateMaxLength(allPositions: Boolean): Int = {
    if (! allPositions)
      return events.length

    val firstBlockMaxLength = startPosition - 1
    val lastBlockMaxLength = contigLen - maxPosition
    firstBlockMaxLength + events.length + lastBlockMaxLength

  }

  def updateEvents(pos: Int, startPart: Int, delta: Short): Unit = {
    val position = pos - startPart
    events(position) = (events(position) + delta).toShort
  }

  def updateAlts(pos: Int, startPart: Int, alt: Char): Unit = {
//    val position = pos - startPart - 1
    val position = pos // naturally indexed
    val altByte = alt.toByte

    val altMap = alts.getOrElse(position, new SingleLocusAlts())
    altMap(altByte) = (altMap.getOrElse(altByte, 0.toShort) + 1).toShort
    alts.update(position, altMap)
  }

  def getTail:Tail ={
    val tailStartIndex = maxPosition - maxSeqLen
    val tailCov = TailCovTimer.time {
      if(maxSeqLen ==  events.length ) events
      else
        events.takeRight(maxSeqLen)
    }
    val tailAlts = TailAltsTimer.time {
      alts.filter(_._1 >= tailStartIndex)
    }
    val cumSum = FastMath.sumShort(events)
    val tail = TailEdgeTimer.time {
      Tail(contig, startPosition, tailStartIndex, tailCov, tailAlts, cumSum)
    }
    tail
  }
}
