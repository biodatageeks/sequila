package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.Cigar


case class ReadSummary(start: Int, end: Int,
                       bases: Array[Byte],
                       quals: Array[Byte],
                       isPositive: Boolean,
                       cigar: Cigar
                            ) {
  private var _cigarConf: CigarDerivedConf = null

  def cigarConf:CigarDerivedConf  = {
    if (_cigarConf == null)
      _cigarConf = CigarDerivedConf.create(start,cigar)
    _cigarConf
  }

  def getBaseForAbsPosition (absPosition:Int):Char = {
    val relPos = relativePosition(absPosition)
    if (isPositive) bases(relPos).toChar else bases(relPos).toChar.toLower
  }

  @inline
  def getBaseQualityForPosition(position: Int): Byte = {
    quals(expensiveRelativePosition(position))
  }
  @inline
  def relativePosition(absPosition:Int):Int = {
    if (!cigarConf.hasIndel && !cigarConf.hasClip) absPosition - start
    else expensiveRelativePosition(absPosition)
  }

  @inline
  def expensiveRelativePosition(absPosition: Int): Int = {
    absPosition - start + inDelEventsOffset(absPosition) + cigarConf.leftClipLength
  }

  @inline
  private def inDelEventsOffset(pos: Int): Int = {
    if (!cigarConf.hasIndel)
      return 0
    cigarConf.getInsertOffsetForPosition(pos)- cigarConf.getDelOffsetForPosition(pos)
  }

  @inline
  def hasDeletionOnPosition(pos: Int): Boolean = {
    val leftClipLen = cigarConf.leftClipLength
    if (!cigarConf.hasDel)
      false
    else
      cigarConf
        .indelPositions
        .delPositions.exists { case (start, end) => pos + leftClipLen >= start && pos + leftClipLen < end }
  }
}

