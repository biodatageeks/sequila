package org.biodatageeks.sequila.pileup.model



case class ReadSummary(start: Int, end: Int,
                       basesArray: Array[Byte],
                       qualsArray: Array[Byte],
                       isPositiveStrand: Boolean,
                       cigarDerivedConf: CigarDerivedConf
                            ) {

  @inline
  def getBaseQualityForPosition(position: Int): Byte = {
    qualsArray(relativePosition(position))
  }

  @inline
  def relativePosition(absPosition: Int): Int = {
    absPosition - start + inDelEventsOffset(absPosition) + cigarDerivedConf.leftClipLength
  }

  @inline
  private def inDelEventsOffset(pos: Int): Int = {
    if (!cigarDerivedConf.hasIndel)
      return 0
    cigarDerivedConf.getInsertOffsetForPosition(pos)- cigarDerivedConf.getDelOffsetForPosition(pos)
  }

  @inline
  def hasDeletionOnPosition(pos: Int): Boolean = {
    val leftClipLen = cigarDerivedConf.leftClipLength
    if (!cigarDerivedConf.hasDel)
      false
    else {
      cigarDerivedConf
        .indelPositions
        .delPositions.exists { case (start, end) => pos + leftClipLen >= start && pos + leftClipLen < end }
    }
  }
}

