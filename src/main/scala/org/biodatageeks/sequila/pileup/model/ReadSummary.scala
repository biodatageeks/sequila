package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}

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

  def calculatePositionInReadSeq(pos: Int): Int = {
    if (!cigar.containsOperator(CigarOperator.INSERTION))
      return pos

    var numInsertions = 0
    val cigarIterator = cigar.iterator()
    var position = 0

    while (cigarIterator.hasNext) {
      if (position > pos + numInsertions)
        return pos + numInsertions
      val cigarElement = cigarIterator.next()
      val cigarOpLength = cigarElement.getLength
      val cigarOp = cigarElement.getOperator

      if (cigarOp == CigarOperator.INSERTION)
        numInsertions += cigarOpLength
      else if (cigarOp != CigarOperator.HARD_CLIP)
        position = position + cigarOpLength
    }
    pos + numInsertions
  }
}

