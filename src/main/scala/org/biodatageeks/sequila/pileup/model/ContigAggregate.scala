package org.biodatageeks.sequila.pileup.model

import org.apache.spark.broadcast.Broadcast

import org.biodatageeks.sequila.pileup.conf.Conf

import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.pileup.model.Quals._



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
                            conf: Broadcast[Conf]
                                ) {

  def hasAltOnPosition(pos:Int):Boolean = alts.contains(pos)
  def trimQuals: MultiLociQuals = if(quals != null) quals.trim(conf) else null

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
  }

  def updateQuals(pos: Int, alt: Char, quality: Byte, firstUpdate: Boolean = false, updateMax:Boolean = true): Unit = {
      quals.updateQuals(pos, alt,quality, firstUpdate, updateMax, conf)
  }
}
