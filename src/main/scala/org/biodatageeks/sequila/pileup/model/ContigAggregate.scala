package org.biodatageeks.sequila.pileup.model

import org.apache.spark.broadcast.Broadcast
import org.biodatageeks.sequila.pileup.broadcast
import org.biodatageeks.sequila.pileup.broadcast.Correction.PartitionCorrections
import org.biodatageeks.sequila.pileup.broadcast.Shrink.PartitionShrinks
import org.biodatageeks.sequila.pileup.broadcast.{FullCorrections, PileupUpdate, Tail}
import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.pileup.timers.PileupTimers.{CalculateAltsTimer, CalculateEventsTimer, CalculateQualsTimer, FillQualityForHigherAltsTimer, FillQualityForLowerAltsTimer, ShrinkArrayTimer, TailAltsTimer, TailCovTimer, TailEdgeTimer}
import org.biodatageeks.sequila.utils.FastMath

import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.pileup.model.Quals._
import scala.collection.{SortedSet, mutable}
import scala.collection.mutable.ArrayBuffer


/** Events aggregation on contig
  */

case class ContigAggregate(
                            contig: String = "",
                            contigLen: Int = 0,
                            events: Array[Short],
                            alts: MultiLociAlts,
                            quals: MultiLociQuals,
                            startPosition: Int = 0,
                            maxPosition: Int = 0,
                            shrinkedEventsArraySize: Int = 0,
                            maxSeqLen:Int = 0,
                            qualityCache: QualityCache
                                ) {

  val altsKeyCache  = mutable.TreeSet.empty[Int]

  def hasAltOnPosition(pos:Int):Boolean = alts.contains(pos)
  def getRange: broadcast.Range = broadcast.Range(contig, startPosition, maxPosition)
  def getPileupUpdate:PileupUpdate = new PileupUpdate(ArrayBuffer(getTail), ArrayBuffer(getRange))
  def getAltPositionsForRange(start: Int, end: Int): SortedSet[Int] = altsKeyCache.range(start,end+1)
  def addToCache(readQualSummary: ReadQualSummary):Unit = qualityCache.addOrReplace(readQualSummary)
  def trimQuals: MultiLociQuals = if(quals != null) quals.trim else null

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

  def updateAlts(pos: Int, alt: Char): Unit = {
    alts.updateAlts(pos, alt)
    if(Conf.includeBaseQualities) {
      altsKeyCache.add(pos)
    }
  }

  def updateQuals(pos: Int, alt: Char, quality: Byte, firstUpdate: Boolean = false, updateMax:Boolean = true): Unit = {
      quals.updateQuals(pos, alt,quality, firstUpdate, updateMax)
  }

  def getTail:Tail ={
    val tailStartIndex = maxPosition - maxSeqLen
    val tailCov = TailCovTimer.time {
      if(maxSeqLen ==  events.length ) events
      else
        events.takeRight(maxSeqLen)
    }
    val tailAlts = TailAltsTimer.time {alts.filter(_._1 >= tailStartIndex)}
    val tailQuals = if (Conf.includeBaseQualities) quals.filter(_._1 >= tailStartIndex) else null
    val cumSum = FastMath.sumShort(events)
    val tail = TailEdgeTimer.time {broadcast.Tail(contig, startPosition, tailStartIndex, tailCov, tailAlts, tailQuals,cumSum, qualityCache)}
    tail
  }

  def calculateAdjustedQuals(upd: PartitionCorrections): MultiLociQuals = {
    if (Conf.includeBaseQualities) {
      calculateCompleteQuals(upd)
    } else
      new MultiLociQuals()
  }

  def getAdjustedAggregate(b:Broadcast[FullCorrections]): ContigAggregate = {
    val upd: PartitionCorrections = b.value.corrections
    val shrink:PartitionShrinks = b.value.shrinks

    val adjustedEvents = CalculateEventsTimer.time { calculateAdjustedEvents(upd) }
    val adjustedAlts = CalculateAltsTimer.time{ calculateAdjustedAlts(upd) }
    val newQuals = CalculateQualsTimer.time {calculateAdjustedQuals(upd)}

    val shrinkedEventsSize = ShrinkArrayTimer.time { calculateShrinkedEventsSize(shrink, adjustedEvents) }
    ContigAggregate(contig, contigLen, adjustedEvents, adjustedAlts, newQuals, startPosition, maxPosition, shrinkedEventsSize, maxSeqLen, null)
  }

  private def calculateAdjustedEvents(upd: PartitionCorrections): Array[Short] = {
    var eventsArrMutable = events

    upd.get((contig, startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
      case Some(correction) => // array of covs and cumSum
        correction.events match {
          case Some(overlapArray) =>
            if (overlapArray.length > events.length)
              eventsArrMutable =  events ++ Array.fill[Short](overlapArray.length - events.length)(0) // extend array

            var i = 0
            eventsArrMutable(i) = (eventsArrMutable(i) + correction.cumulativeSum).toShort // add cumSum to zeroth element

            while (i < overlapArray.length) {
              eventsArrMutable(i) = (eventsArrMutable(i) + overlapArray(i)).toShort
              i += 1
            }
            eventsArrMutable
          case None =>
            eventsArrMutable(0) = (eventsArrMutable(0) + correction.cumulativeSum).toShort
            eventsArrMutable
        }
      case None => eventsArrMutable
    }
  }

  private def calculateAdjustedAlts(upd:PartitionCorrections): MultiLociAlts = {
    upd.get((contig, startPosition)) match { // check if there is a value for contigName and minPos in upd, returning correction object
      case Some(correction) =>
        correction.alts match {
          case Some(overlapAlts) => alts.merge(overlapAlts)
          case None => alts
        }
      case None => alts
    }
  }

  def fillQualityForHigherAlts(upd: PartitionCorrections, adjustedQuals: MultiLociQuals, blacklist: scala.collection.Set[Int]): MultiLociQuals = {

    upd.get((contig, startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
      case Some(correction) =>
        correction.events match {
          case Some(overlapArray) => {// fill BQ for alts in new Partition with cache from correction broadcast
            val qualStart = startPosition
            val qualEnd = startPosition+ overlapArray.length
            val qualsSet = alts.keySet.filter(pos=> pos >= qualStart && pos < qualEnd).diff(blacklist)

            for (pos <- qualsSet) {
              val reads = correction.qualityCache.getReadsOverlappingPosition(pos)
              for (read <- reads) {
                val qual = read.getBaseQualityForPosition(pos.toInt)
                adjustedQuals.updateQuals(pos.toInt, QualityConstants.REF_SYMBOL, qual, firstUpdate = false, updateMax = false)
              }
            }
            adjustedQuals
          }
          case None => adjustedQuals
        }
      case None => adjustedQuals
    }


  }

  def fillQualityForLowerAlts(upd: PartitionCorrections, qualsInterim: MultiLociQuals, blacklist: scala.collection.Set[Int]): MultiLociQuals ={

    upd.get((contig, startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
      case Some(correction) =>
        correction.alts match {
          case Some(alts) => {
            // fill BQ for alts in old Partition with cache from aggregate cache
            val qualsSet = alts.keySet.diff(blacklist)
            for (pos <- qualsSet) {
              val reads = qualityCache.getReadsOverlappingPositionFullCache(pos)
              for (read <- reads) {
                val qual = read.getBaseQualityForPosition(pos.toInt)
                qualsInterim.updateQuals(pos.toInt, QualityConstants.REF_SYMBOL, qual, firstUpdate = false, updateMax = false)
              }
            }
            qualsInterim
          }
          case None => qualsInterim
        }
      case None => qualsInterim
    }
  }

  private def calculateCompleteQuals(upd:PartitionCorrections): MultiLociQuals ={

    val adjustedQuals = upd.get((contig, startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
      case Some(correction) =>
        correction.quals match {
          case Some(overlapQuals) => quals.merge(overlapQuals)
          case None => quals
        }
      case None => quals
    }

    val concordantAlts = quals.keySet.intersect(upd.getAlts(contig,startPosition).keySet)

    val qualsInterim = FillQualityForHigherAltsTimer.time{ fillQualityForHigherAlts(upd, adjustedQuals, concordantAlts)}
    val completeQuals = FillQualityForLowerAltsTimer.time {fillQualityForLowerAlts(upd, qualsInterim, concordantAlts)}
    completeQuals
  }

  private def calculateShrinkedEventsSize[T](shrink: PartitionShrinks, updArray: Array[T]): Int = {
    shrink.get((contig, startPosition)) match {
      case Some(shrink) => shrink.index
      case None => updArray.length
    }
  }
}
