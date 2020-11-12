package org.biodatageeks.sequila.pileup.model



case class ReadQualSummary (start: Int, end: Int,
                            qualsArray: Array[Byte],
                            cigarDerivedConf: CigarDerivedConf
                            ) {

  @inline
  def getBaseQualityForPosition(position: Int): Byte = {
    qualsArray(relativePosition(position))
  }

  @inline
  def overlapsPosition(pos: Int): Boolean = !hasDeletionOnPosition(pos) && start <= pos && end >= pos

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
    if (!cigarDerivedConf.hasDel)
      false
    else {
      cigarDerivedConf
        .indelPositions
        .delPositions.exists { case (start, end) => pos >= start && pos < end }
    }
  }
}

