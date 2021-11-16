package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeRedBlack

case class InDelPositions(
                           delPositions:IntervalTreeRedBlack[(Int)],
                           insertPositions: ListBuffer[(Int,Int)]
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
      .filter{case (start, _) => (pos >= start )}
    val lengths = filtered.map(_._2)
    val lenSum = lengths.sum
    lenSum
  }
  def getDelOffsetForPosition(position:Int): Int = {
    val pos = position + leftClipLength
    val delIterator = indelPositions.delPositions.overlappersWithoutEnd(pos)
    var lenSum = 0
    while (delIterator.hasNext){
      val next = delIterator.next()
      lenSum += next.getValue.get(0)
    }
    lenSum
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
    val delPositions = new IntervalTreeRedBlack[(Int)]()
    val insertPositions  = new mutable.ListBuffer[(Int,Int)]()
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
          delPositions.put(eventStart, eventEnd, eventEnd - eventStart)
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