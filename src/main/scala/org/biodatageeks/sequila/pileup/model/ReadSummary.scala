package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.Cigar

import scala.util.control.Breaks.{break, breakable}


case class ReadSummary(start: Int, end: Int,
                       bases: Array[Byte],
                       quals: Array[Byte],
                       isPositive: Boolean,
                       cigar: Cigar
                            ) {
  private var _cigarConf: CigarDerivedConf = null

  private var insertCumSum: Int = 0
  private var insertPos: Int = 0

  private var delCumSum: Int = 0
  private var delPos: Int = 0

  private var hasDelPos: Int = 0

  def resetCumState(): Unit = {
    delCumSum = 0
    delPos = 0
    insertCumSum = 0
    insertPos = 0
    hasDelPos = 0
  }

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
    getInsertOffsetForPosition(pos)- getDelOffsetForPosition(pos)
  }


  def hasDeletionOnPosition(position:Int): Boolean = {
    val pos = position + cigarConf.leftClipLength
    if (!cigarConf.hasDel)
      return false
    val arr = cigarConf.indelPositions.delPositions
    var i = hasDelPos
    var res = false
    breakable{
      while(i < arr.length){
        if(pos >= arr(i)._1 && pos < arr(i)._2)
          res = true
        else
          break
        i+=1
      }
    }
    hasDelPos = i
    res
  }

  def getInsertOffsetForPosition(position:Int): Int = {
    val pos = position + cigarConf.leftClipLength
    val arr = cigarConf.indelPositions.insertPositions
    var i = insertPos
    var sum = insertCumSum
    breakable{
      while(i < arr.length){
        if(pos >= arr(i)._1)
          sum += arr(i)._2
        else
          break
        i+=1
      }
    }
    insertCumSum = sum
    insertPos = i
    sum
  }

  def getDelOffsetForPosition(position:Int): Int = {
    val pos = position + cigarConf.leftClipLength
    val arr = cigarConf.indelPositions.delPositions
    var i = delPos
    var sum = delCumSum
    breakable {
      while (i < arr.length) {
        if (pos >= arr(i)._1)
          sum += (arr(i)._2 - arr(i)._1 )
        else
          break
        i += 1
      }
    }
    delCumSum = sum
    delPos = i
    sum
  }
}

