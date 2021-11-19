package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}

import scala.collection.mutable

import scala.collection.mutable.{ArrayBuffer}

import scala.util.control.Breaks.{break, breakable}

case class InDelPositions(
                           delPositions:ArrayBuffer[(Int, Int)],
                           insertPositions: ArrayBuffer[(Int,Int)]
                         )
case class CigarDerivedConf(
                             hasClip: Boolean,
                             hasIndel: Boolean,
                             hasDel: Boolean,
                             leftClipLength: Int,
                             indelPositions: InDelPositions = null
                           ) {
  private var insertCumSum: Int = 0
  private var insertPos: Int = 0

  private var delCumSum: Int = 0
  private var delPos: Int = 0

  def getInsertOffsetForPosition(position:Int): Int = {
    val pos = position + leftClipLength
    val arr = indelPositions.insertPositions
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
    val pos = position + leftClipLength
    val arr = indelPositions.delPositions
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

object CigarDerivedConf {
  def create(start: Int, cigar:Cigar): CigarDerivedConf ={
    val firstCigarElement = cigar.getFirstCigarElement
    val firstCigarElementOp = firstCigarElement.getOperator
    val hasClip = firstCigarElementOp != null && firstCigarElementOp==CigarOperator.SOFT_CLIP
    val softClipLength = if(hasClip) {
      firstCigarElement.getLength
    } else 0
    val hasDel = cigar.containsOperator(CigarOperator.DELETION)
    val hasIndel =  hasDel || cigar.containsOperator(CigarOperator.INSERTION)
    CigarDerivedConf(hasClip, hasIndel, hasDel, softClipLength, if (hasIndel) calculateIndelPositions(start, cigar) else null)
  }


  private def calculateIndelPositions(start: Int, cigar:Cigar): InDelPositions = {
    val delPositions = new ArrayBuffer[(Int, Int)]()
    val insertPositions  = new ArrayBuffer[(Int,Int)]()
    val cigarIterator = cigar.iterator()
    var positionFromCigar = start
    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator
      if (cigarOperator == CigarOperator.DELETION || cigarOperator == CigarOperator.INSERTION) {
        val eventStart = positionFromCigar
        val eventEnd = positionFromCigar + cigarOperatorLen
        if (cigarOperator == CigarOperator.DELETION)
          delPositions.append((eventStart, eventEnd) )
        else if (cigarOperator == CigarOperator.INSERTION){
          insertPositions.append((eventStart, cigarOperatorLen))
        }
      }
      if (cigarOperator != CigarOperator.INSERTION && cigarOperator != CigarOperator.HARD_CLIP)
        positionFromCigar += cigarOperatorLen
    }
    InDelPositions(delPositions, insertPositions)
  }
}