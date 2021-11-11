package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.Cigar


case class ReadSummary(start: Int, end: Int,
                       basesArray: Array[Byte],
                       qualsArray: Array[Byte],
                       isPositiveStrand: Boolean,
                       cigar: Cigar
                            ) {
  private var _cigarDerivedConf: CigarDerivedConf = null

  def cigarDerivedConf:CigarDerivedConf  = {
    if (_cigarDerivedConf == null)
      _cigarDerivedConf = CigarDerivedConf.create(start,cigar)
    _cigarDerivedConf
  }

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
      val postition =  pos + leftClipLen
      if(cigarDerivedConf
        .indelPositions
        .delPositions.minOverlapperExcludeLeft(postition, postition) != null)
        true
      else
        false

    }
  }
}

