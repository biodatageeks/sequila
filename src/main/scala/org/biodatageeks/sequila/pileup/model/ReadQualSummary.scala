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
    if(!cigarDerivedConf.hasClip)
      absPosition - start + inDelEventsOffset(absPosition)
    else
      absPosition - start + inDelEventsOffset(absPosition) + cigarDerivedConf.leftClipLength
  }

  @inline
  private def inDelEventsOffset(pos: Int): Int = {
    if (!cigarDerivedConf.hasIndel)
      return 0
    cigarDerivedConf
      .indelPositions
      .insertPositions
      .count(_ <= pos + cigarDerivedConf.leftClipLength) -
      cigarDerivedConf
      .indelPositions
      .delPositions
      .count(_ <= pos + cigarDerivedConf.leftClipLength)

  }

  @inline
  def hasDeletionOnPosition(pos: Int): Boolean = {
    if (!cigarDerivedConf.hasDel)
      false
    else {
      cigarDerivedConf
        .indelPositions
        .delPositions.contains(pos)
    }
  }
}

