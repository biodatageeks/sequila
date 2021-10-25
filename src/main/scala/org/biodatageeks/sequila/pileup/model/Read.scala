package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator, SAMRecord}
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.pileup.model.Quals._
import org.biodatageeks.sequila.pileup.model.Alts._

import scala.collection.mutable

object ReadOperations {

  object implicits {
    implicit def reads(r: SAMRecord) = ExtendedReads(r)
  }
}
case class TruncRead(rName: String, contig: String, posStart: Int, posEnd: Int)
case class ExtendedReads(read: SAMRecord) {


  def analyzeRead( agg: ContigAggregate): Unit = {
    val start = read.getStart
    val cigar = read.getCigar
    val isPositiveStrand = ! read.getReadNegativeStrandFlag

    val newMaxEvent = calculateEvents(agg, start, cigar)
    calculateAlts(agg, start, cigar, isPositiveStrand)

    if (agg.conf.includeBaseQualities)
      calculateQuals (agg, start, cigar, read.getBaseQualities, isPositiveStrand, newMaxEvent)
  }

  def calculateQuals(agg: ContigAggregate, start: Int, cigar: Cigar, bQual: Array[Byte], isPositiveStrand:Boolean, newMax: Int):Unit = {
    if (newMax > agg.maxEventPosition) {
      agg.extendAllocation (newMax)
      agg.maxEventPosition = newMax
    }

    val cigarConf = CigarDerivedConf.create(start, cigar)
    val readQualSummary = ReadSummary(start, read.getEnd, read.getReadBases, bQual, cigarConf)
    fillBaseQualities(agg, readQualSummary, isPositiveStrand)
  }


  def calculateEvents(aggregate: ContigAggregate, start: Int, cigar: Cigar): Int = {
    val partitionStart = aggregate.startPosition
    var position = start
    val cigarIterator = cigar.iterator()
    var cigarLen = 0

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator

      if (cigarOperator == CigarOperator.M ||
        cigarOperator == CigarOperator.X ||
        cigarOperator == CigarOperator.EQ ||
        cigarOperator == CigarOperator.N ||
        cigarOperator == CigarOperator.D)
        cigarLen += cigarOperatorLen

      // update events array according to read alignment blocks start/end
      if (cigarOperator == CigarOperator.M || cigarOperator == CigarOperator.X || cigarOperator == CigarOperator.EQ) {

        aggregate.updateEvents(position, partitionStart, delta = 1)
        position += cigarOperatorLen
        aggregate.updateEvents(position, partitionStart, delta = -1)
      }
      else if (cigarOperator == CigarOperator.N || cigarOperator == CigarOperator.D)
        position += cigarOperatorLen
    }
    position
  }


  def calculatePositionInReadSeq(mdPosition: Int, cigar: Cigar): Int = {
    if (!cigar.containsOperator(CigarOperator.INSERTION))
      return mdPosition

    var numInsertions = 0
    val cigarIterator = cigar.iterator()
    var position = 0

    while (cigarIterator.hasNext) {
      if (position > mdPosition + numInsertions)
        return mdPosition + numInsertions
      val cigarElement = cigarIterator.next()
      val cigarOpLength = cigarElement.getLength
      val cigarOp = cigarElement.getOperator

      if (cigarOp == CigarOperator.INSERTION)
        numInsertions += cigarOpLength
      else if (cigarOp != CigarOperator.HARD_CLIP)
        position = position + cigarOpLength
    }
    mdPosition + numInsertions
  }

  def calculateAlts(aggregate: ContigAggregate, start: Int,
                    cigar: Cigar,
                    isPositiveStrand:Boolean): Unit = {
    var position = start
    val ops = MDTagParser.parseMDTag(read.getStringAttribute("MD"))

    var delCounter = 0
    val altsPositions = mutable.Set.empty[Int]
    val clipLen =
      if (cigar.getCigarElement(0).getOperator == CigarOperator.SOFT_CLIP)
        cigar.getCigarElement(0).getLength else 0

    position += clipLen

    for (mdtag <- ops) {
      if (mdtag.isDeletion) {
        delCounter += 1
        position += 1
      } else if (mdtag.base != 'S') {
        position += 1

        val indexInSeq = calculatePositionInReadSeq(position - start - delCounter, cigar)
        val altBase = if (isPositiveStrand) read.getReadString.charAt(indexInSeq - 1).toUpper else read.getReadString.charAt(indexInSeq - 1).toLower
        val altPosition = position - clipLen - 1

        aggregate.alts.updateAlts(altPosition, altBase)
        altsPositions += altPosition
      }
      else if (mdtag.base == 'S')
        position += mdtag.length
    }
  }

  def fillBaseQualities(agg: ContigAggregate, readSummary: ReadSummary, isPositive:Boolean): Unit = {
    val start = readSummary.start
    val end = readSummary.end
    var currPosition = start
    while (currPosition <= end) {
      if (!readSummary.hasDeletionOnPosition(currPosition)) {
        val relativePos = if (!readSummary.cigarDerivedConf.hasIndel && !readSummary.cigarDerivedConf.hasClip) currPosition - readSummary.start
        else readSummary.relativePosition(currPosition)
          val base = if(isPositive)  readSummary.basesArray(relativePos).toChar.toUpper else readSummary.basesArray(relativePos).toChar.toLower
          agg.quals.updateQuals(currPosition, base, readSummary.qualsArray(relativePos), agg.conf)
      }
      currPosition += 1
    }
  }
}
