package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}

import scala.collection.{SortedSet, mutable}

case class InDelPositions(
                           delPositions: mutable.LinkedHashSet[Int],
                           insertPositions: mutable.LinkedHashSet[Int]
                         )
case class CigarDerivedConf(
                             hasClip: Boolean,
                             hasIndel: Boolean,
                             hasDel: Boolean,
                             leftClipLength: Int,
                             indelPositions: InDelPositions = null
                           )

object CigarDerivedConf {
  def create(start: Int, cigar:Cigar) ={
    val hasClip = cigar.isLeftClipped //FIXME: to verify
    val softClipLength = if(hasClip) {
      val cigarElement = cigar.getFirstCigarElement
      cigarElement.getLength
    } else 0
    val hasDel = cigar.containsOperator(CigarOperator.DELETION)
    val hasIndel =  hasDel || cigar.containsOperator(CigarOperator.INSERTION)
    CigarDerivedConf(hasClip, hasIndel, hasDel, softClipLength, if (hasIndel) getIndelPostions(start, cigar) else null)
  }


  private def getIndelPostions(start: Int, cigar:Cigar): InDelPositions = {
    val delPositions = new mutable.LinkedHashSet[Int]()
    val insertPositions  = new mutable.LinkedHashSet[Int]()
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
          fillPositionSet(eventStart, eventEnd, delPositions)
        else if (cigarOperator == CigarOperator.INSERTION){
         fillPositionSet(eventStart, eventEnd, insertPositions)
        }
      }
      positionFromCigar += cigarOperatorLen
   }
   InDelPositions(delPositions, insertPositions)
  }

  private def fillPositionSet(start:Int, end: Int, set: mutable.LinkedHashSet[Int]) = {
    var i = start
    while(i < end) {
      set.add(i)
      i += 1
    }
    set
  }
}