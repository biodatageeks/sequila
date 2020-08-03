package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}

case class EventsForReadPosition(hasDeletion:Boolean, numDeletionsBefore: Int, numInsertionsBefore:Int)

case class ReadQualSummary (start: Int, end: Int, qualString: String, cigar: Cigar) {

  def getBaseQualityForPosition(position: Int): Short = {
    qualString.charAt(relativePosition(position)).toShort
  }

  def overlapsPosition(pos:Long):Boolean = isPositionInRange(pos) && !hasDeletionOnPosition(pos)

  def relativePosition(absPosition: Int):Int = {
    absPosition - start + inDelEventsOffset(absPosition) + leftClipLength
  }

  private def leftClipLength: Int = {
    val cigarElement = cigar.getFirstCigarElement
    if(cigarElement.getOperator== CigarOperator.SOFT_CLIP || cigarElement.getOperator==CigarOperator.HARD_CLIP)
      cigarElement.getLength
    else
      0
  }

  private def isPositionInRange(pos:Long) = start<= pos && end >= pos

  private def inDelEventsOffset(pos: Int):Int = {
    if(!cigar.containsOperator(CigarOperator.DELETION) && !cigar.containsOperator(CigarOperator.INSERTION))
      return 0

    val eventsForPosition = traverseCigar(cigar, pos, countEventsBeforePosition = true)
    eventsForPosition.numInsertionsBefore - eventsForPosition.numDeletionsBefore
  }

  def hasDeletionOnPosition(pos:Long):Boolean = {
    if(!cigar.containsOperator(CigarOperator.DELETION))
      return false

    traverseCigar(cigar,pos,hasDelOnPosition = true).hasDeletion
  }

  private def traverseCigar(cigar:Cigar, pos: Long, countEventsBeforePosition: Boolean = false, hasDelOnPosition: Boolean=false): EventsForReadPosition = {
    val cigarIterator = cigar.iterator()
    var positionFromCigar = start
    var numDeletions = 0
    var numInsertions =0

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator

      if (cigarOperator == CigarOperator.DELETION || cigarOperator == CigarOperator.INSERTION) {
        val eventStart = positionFromCigar
        val eventEnd = positionFromCigar + cigarOperatorLen

        if (pos >= eventStart && pos < eventEnd && countEventsBeforePosition) {
          val diff = pos.toInt - eventStart
          numDeletions = if (cigarOperator == CigarOperator.DELETION) numDeletions + diff else numDeletions
          numInsertions = if (cigarOperator == CigarOperator.INSERTION) numInsertions + math.max(diff, 1) else numInsertions
          // if insertion happens on this position (diff == 0) we have to count it in
          // this doesn't apply to deletions

          return  EventsForReadPosition(false, numDeletions, numInsertions)
        }
        if (pos >= eventStart && pos < eventEnd && hasDelOnPosition)
          return  EventsForReadPosition(true, 0,0)

        numDeletions = if (cigarOperator == CigarOperator.DELETION) numDeletions+ cigarOperatorLen else numDeletions
        numInsertions = if (cigarOperator == CigarOperator.INSERTION) numInsertions+ cigarOperatorLen else numInsertions

      }

      positionFromCigar += cigarOperatorLen

      if (positionFromCigar > pos + leftClipLength && hasDelOnPosition)
        return  EventsForReadPosition(false, 0,0)

      if (positionFromCigar > pos + leftClipLength && countEventsBeforePosition)
        return  EventsForReadPosition(false, numDeletions, numInsertions)
    }
    EventsForReadPosition(false, 0 , 0)
  }

}


