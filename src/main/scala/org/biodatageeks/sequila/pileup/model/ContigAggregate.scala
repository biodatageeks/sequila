package org.biodatageeks.sequila.pileup.model


import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeRedBlack


/** Events aggregation on contig
 */

case class ContigAggregate(
                            contig: String = "",
                            contigLen: Int = 0,
                            events: Array[Short],
                            alts: MultiLociAlts,
                            rsTree: IntervalTreeRedBlack[ReadSummary],
                            startPosition: Int = 0,
                            conf: Conf
                          ) {

  @inline
  def hasAltOnPosition(pos:Int):Boolean = alts.contains(pos)

  def calculateMaxLength(allPositions: Boolean): Int = {
    if (! allPositions)
      return events.length
    events.length + 2
  }

  def updateEvents(pos: Int, startPart: Int, delta: Short): Unit = {
    val position = pos - startPart
    if (position > events.length -1)
      return
    events(position) = (events(position) + delta).toShort
  }

  @inline
  def addReadToBuffer(rs: ReadSummary): Unit = {
    if (!conf.includeBaseQualities)
      return
    rs.cigarConf.resetCumState
    rsTree.put(rs.start, rs.end, rs)
    ()
  }

}
