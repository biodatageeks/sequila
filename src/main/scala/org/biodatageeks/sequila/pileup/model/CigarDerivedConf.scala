package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeRedBlack

import scala.collection.mutable

case class InDelPositions(
                           delPositions:IntervalTreeRedBlack[(Int)],
                           insertPositions: mutable.LinkedHashSet[(Int,Int)]
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
      .filter{case (start,len) => (pos >= start )}.toList
    val lengths = filtered.map(_._2)
    val lenSum = lengths.sum
    lenSum
  }

  def getDelOffsetForPosition(position:Int): Int = {
    val pos = position + leftClipLength
    val delIterator = indelPositions.delPositions.iterator()
    var lenSum = 0
    while (delIterator.hasNext){
      val next = delIterator.next()
      if(pos >= next.getStart)
        lenSum += next.getValue.get(0)
    }
    lenSum
  }
}

object CigarDerivedConf {
  def create(start: Int, cigar:Cigar) ={
    val hasClip = cigar.isLeftClipped
    val softClipLength = if(hasClip) {
      val cigarElement = cigar.getFirstCigarElement
      cigarElement.getLength
    } else 0
    val hasDel = cigar.containsOperator(CigarOperator.DELETION)
    val hasIndel =  hasDel || cigar.containsOperator(CigarOperator.INSERTION)
    CigarDerivedConf(hasClip, hasIndel, hasDel, softClipLength, if (hasIndel) getIndelPostions(start, cigar) else null)
  }


  private def getIndelPostions(start: Int, cigar:Cigar): InDelPositions = {
    val delPositions = new IntervalTreeRedBlack[(Int)]()
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
          delPositions.put(eventStart, eventEnd, eventEnd - eventStart)
          //fillPositionSet(eventStart, eventEnd, delPositions)
        else if (cigarOperator == CigarOperator.INSERTION){
          insertPositions.add((eventStart, cigarOperatorLen))
         //fillPositionSet(eventStart, eventEnd, insertPositions)
        }
      }
      if (cigarOperator != CigarOperator.INSERTION)
        positionFromCigar += cigarOperatorLen
   }
   InDelPositions(delPositions, insertPositions)
  }


  private def fillPositionSet(start:Int, end: Int, set: mutable.LinkedHashSet[Int]): mutable.LinkedHashSet[Int] = {
    var i = start
    while(i < end) {
      set.add(i)
      i += 1
    }
    set
  }
}