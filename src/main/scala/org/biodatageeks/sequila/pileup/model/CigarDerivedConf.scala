package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}

import scala.collection.mutable

case class InDelPositions(
                           delPositions: List[(Int, Int)],
                           insertPositions: List[(Int,Int)]
                         )
case class CigarDerivedConf(
                             hasClip: Boolean,
                             hasIndel: Boolean,
                             hasDel: Boolean,
                             leftClipLength: Int,
                             indelPositions: InDelPositions = null
                           ) {
  def getInsertOffsetForPosition(position:Int): Int = {
    val pos = position + leftClipLength
    val filtered = indelPositions
      .insertPositions
      .filter{case (start, _) => (pos >= start )}.toList
    val lengths = filtered.map(_._2)
    val lenSum = lengths.sum
    lenSum
  }
  def getDelOffsetForPosition(position:Int): Int = {
    val pos = position + leftClipLength
    indelPositions
      .delPositions
      .filter{case (start,_) => pos >=start}
      .map{case(start,end)=> end-start}
      .sum
  }
}

object CigarDerivedConf {
  def create(start: Int, cigar:Cigar): CigarDerivedConf ={
    val firstCigarElement = cigar.getFirstCigarElement
    val firstCigarElementOp = firstCigarElement.getOperator
    val hasClip = firstCigarElementOp != null && firstCigarElementOp.isClipping
    val softClipLength = if(hasClip) {
      firstCigarElement.getLength
    } else 0
    val hasDel = cigar.containsOperator(CigarOperator.DELETION)
    val hasIndel =  hasDel || cigar.containsOperator(CigarOperator.INSERTION)
    CigarDerivedConf(hasClip, hasIndel, hasDel, softClipLength, if (hasIndel) calculateIndelPositions(start, cigar) else null)
  }


  private def calculateIndelPositions(start: Int, cigar:Cigar): InDelPositions = {
    val delPositions = new mutable.LinkedHashSet[(Int, Int)]()
    val insertPositions  = new mutable.LinkedHashSet[(Int,Int)]()
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
          delPositions.add((eventStart,eventEnd))
        else if (cigarOperator == CigarOperator.INSERTION){
          insertPositions.add((eventStart, cigarOperatorLen))
        }
      }
      if (cigarOperator != CigarOperator.INSERTION)
        positionFromCigar += cigarOperatorLen
   }
   InDelPositions(delPositions.toList, insertPositions.toList)
  }
}