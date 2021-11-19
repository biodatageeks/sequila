package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}

import scala.collection.mutable

import scala.collection.mutable.ArrayBuffer

case class InDelPositions(
                           delPositions:ArrayBuffer[(Int, Int)],
                           insertPositions: ArrayBuffer[(Int,Int)]
                         )
case class CigarDerivedConf(
                             hasClip: Boolean,
                             hasIndel: Boolean,
                             hasDel: Boolean,
                             leftClipLength: Int,
                             indelPositions: InDelPositions = null,
                             insertsLen: Int,
                             delsLen: Int
                           ) {

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
    val indels = if (hasIndel) calculateIndelPositions(start, cigar) else null
    val insertsLen = if (hasIndel) indels.insertPositions.length else 0
    val delsLen = if (hasIndel) indels.delPositions.length else 0

    CigarDerivedConf(hasClip, hasIndel, hasDel, softClipLength, indels, insertsLen, delsLen)
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