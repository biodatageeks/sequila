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
    if (relPos >= bases.length) {
      println()
      cigarConf.indelPositions.delPositions.printTree()
      val it = cigarConf.indelPositions.delPositions.overlappersWithoutEnd(absPosition + cigarConf.leftClipLength)
      while (it.hasNext) {
        val node = it.next()
        println("NODE " + node)
      }
    }
    val relPos2 = relativePosition(absPosition)
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
    else {
      val postition = pos + leftClipLen
      if (cigarConf
        .indelPositions
        .delPositions.minOverlapperExcludeLeft(postition, postition) != null)
        true
      else
        false
    }
  }
}

