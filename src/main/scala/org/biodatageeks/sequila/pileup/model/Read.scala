package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator, SAMRecord}
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.pileup.model.Alts._

object ReadOperations {

  object implicits {
    implicit def reads(r: SAMRecord) = ExtendedReads(r)
  }
}
case class TruncRead(rName: String, contig: String, posStart: Int, posEnd: Int)
case class ExtendedReads(read: SAMRecord) {


  def addReadToQualsBuffer(agg: ContigAggregate, bases: Array[Byte]): Unit = {
    val readQualSummary = ReadSummary(read.getStart, read.getEnd, bases, read.getBaseQualities, ! read.getReadNegativeStrandFlag, read.getCigar)
    agg.rsTree.put(read.getStart, read.getEnd, readQualSummary)
    ()
  }

  def analyzeReadWithQuals(agg: ContigAggregate): Unit = {
    val bases = read.getReadBases
    calculateEvents(agg)
    calculateAlts(agg, bases)
    addReadToQualsBuffer(agg, bases)
  }

  def analyzeReadNoQuals(agg: ContigAggregate): Unit = {
    val bases = read.getReadBases
    calculateEvents(agg)
    calculateAlts(agg, bases)
  }

  def calculateEvents(aggregate: ContigAggregate): Unit = {
    val start = read.getStart
    val cigar = read.getCigar
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

  def calculateAlts(aggregate: ContigAggregate, bases: Array[Byte]): Unit = {
    val start = read.getStart
    val cigar = read.getCigar
    val isPositiveStrand = !read.getReadNegativeStrandFlag
    var position = start
    val ops = MDTagParser.parseMDTag(read.getStringAttribute("MD"))

    var delCounter = 0
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
        val altBase = if (isPositiveStrand) bases(indexInSeq - 1).toChar else bases(indexInSeq - 1).toChar.toLower
        val altPosition = position - clipLen - 1

        aggregate.alts.updateAlts(altPosition, altBase)
      }
      else if (mdtag.base == 'S')
        position += mdtag.length
    }
  }
}
