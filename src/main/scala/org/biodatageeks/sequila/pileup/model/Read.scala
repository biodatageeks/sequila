package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator, SAMRecord}
import org.apache.spark.broadcast.Broadcast
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.model.Quals._
import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.pileup.conf.QualityConstants.REF_SYMBOL

import scala.collection.mutable

object ReadOperations {

  object implicits {
    implicit def reads(r: SAMRecord) = ExtendedReads(r)
  }

}
case class TruncRead(rName: String, contig: String, posStart: Int, posEnd: Int)

case class ExtendedReads(read: SAMRecord) {

  def analyzeRead(agg: ContigAggregate,
                  conf : Broadcast[Conf]
                 ): Unit = {
    val start = read.getStart
    val cigar = read.getCigar

    val isPositiveStrand = ! read.getReadNegativeStrandFlag

    calculateEvents(agg, start, cigar)
    val altPositions = calculateAlts(agg, start, cigar, isPositiveStrand)

    if (conf.value.includeBaseQualities) {
      val bQual = read.getBaseQualities
      calculateQuals (agg, altPositions, start, cigar, bQual, isPositiveStrand, conf.value)
    }
  }

  def calculateQuals(agg: ContigAggregate, altPositions:scala.collection.Set[Int],start: Int, cigar: Cigar, bQual: Array[Byte], isPositiveStrand:Boolean, conf: Conf):Unit = {
    val cigarConf = CigarDerivedConf.create(start, cigar)
    //val readBases = if (isPositiveStrand) read.getReadBases.map(_.toChar.toUpper) else read.getReadBases.map(_.toChar.toLower)
    val readQualSummary = ReadSummary(start, read.getEnd, read.getReadBases, bQual, cigarConf)
    fillBaseQualities(agg, altPositions, readQualSummary, isPositiveStrand, conf)
  }

  def calculateEvents(aggregate: ContigAggregate,
                      start: Int, cigar: Cigar): Unit = {
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

  def calculateAlts(aggregate: ContigAggregate, start: Int,
                    cigar: Cigar,
                    isPositiveStrand:Boolean): scala.collection.Set[Int] = {
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
    altsPositions
  }


  def fillBaseQualities(agg: ContigAggregate, altPositions:scala.collection.Set[Int], readSummary: ReadSummary, isPositive:Boolean, conf:Conf): Unit = {
    val start = readSummary.start
    val end = readSummary.end
    var currPosition = start
    while (currPosition <= end) {
      if (!readSummary.hasDeletionOnPosition(currPosition)) {
        val relativePos = if (!readSummary.cigarDerivedConf.hasIndel && !readSummary.cigarDerivedConf.hasClip) currPosition - readSummary.start
        else readSummary.relativePosition(currPosition)
        if (altPositions.contains(currPosition)) {
          val base = if(isPositive)  readSummary.basesArray(relativePos).toChar.toUpper else readSummary.basesArray(relativePos).toChar.toLower
          agg.quals.updateQuals(currPosition, base, readSummary.qualsArray(relativePos), conf)
        } else
          agg.quals.updateQuals(currPosition, REF_SYMBOL, readSummary.qualsArray(relativePos), conf)
      }
      currPosition += 1
    }
  }
}
