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

  def analyzeRead(contig: String,
                  agg: ContigAggregate,
                  contigMaxReadLen: mutable.HashMap[String, Int],
                  conf : Broadcast[Conf]
                 ): Unit = {
    val start = read.getStart
    val cigar = read.getCigar
    val bQual = read.getBaseQualities
    val isPositiveStrand = ! read.getReadNegativeStrandFlag

    calculateEvents(contig, agg, contigMaxReadLen, start, cigar)
    val altPositions = calculateAlts(agg, start, cigar, bQual, isPositiveStrand, conf)

    if (conf.value.includeBaseQualities) {
      calculateQuals (agg, altPositions, start, cigar, bQual, isPositiveStrand, conf)
    }
  }

  def calculateQuals(agg: ContigAggregate, altPositions:scala.collection.Set[Int],start: Int, cigar: Cigar, bQual: Array[Byte], isPositiveStrand:Boolean, conf: Broadcast[Conf]):Unit = {
    val cigarConf = CigarDerivedConf.create(start, cigar)
    val readBases = if (isPositiveStrand) read.getReadBases.map(_.toChar.toUpper) else read.getReadBases.map(_.toChar.toLower)
    val readQualSummary = ReadSummary(start, read.getEnd, readBases, bQual, cigarConf)
    fillBaseQualities(agg, altPositions, readQualSummary, conf)
  }

  def calculateEvents(contig: String, aggregate: ContigAggregate, contigMaxReadLen: mutable.HashMap[String, Int],
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
    updateMaxCigarInContig(cigarLen, contig, contigMaxReadLen)
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
                    cigar: Cigar, bQual: Array[Byte],
                    isPositiveStrand:Boolean, conf: Broadcast[Conf]): scala.collection.Set[Int] = {
    var position = start
    val ops = MDTagParser.parseMDTag(read.getStringAttribute("MD"))

    var delCounter = 0
    var altsPositions = mutable.Set.empty[Int]
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
        val altBaseQual = if (conf.value.isBinningEnabled) (bQual(indexInSeq - 1)/conf.value.binSize).toByte else bQual(indexInSeq - 1)
        val altPosition = position - clipLen - 1

        aggregate.alts.updateAlts(altPosition, altBase)
        altsPositions += altPosition
      }
      else if (mdtag.base == 'S')
        position += mdtag.length
    }
    altsPositions
  }

  def updateMaxCigarInContig(cigarLen: Int, contig: String, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {
    if (cigarLen > contigMaxReadLen(contig))
      contigMaxReadLen(contig) = cigarLen
  }


  def fillBaseQualities(agg: ContigAggregate, altPositions:scala.collection.Set[Int], readSummary: ReadSummary, conf: Broadcast[Conf]): Unit = {
    val positionsToFill = (read.getAlignmentStart to read.getAlignmentEnd).toArray
    var ind = 0
    while (ind < positionsToFill.length) {
      val currPosition = positionsToFill(ind)
      if (!readSummary.hasDeletionOnPosition(currPosition)) {
        val relativePos = if (!readSummary.cigarDerivedConf.hasIndel && !readSummary.cigarDerivedConf.hasClip) currPosition - readSummary.start
        else readSummary.relativePosition(currPosition)
        if (altPositions.contains(currPosition))
          agg.quals.updateQuals(currPosition, readSummary.basesArray(relativePos), readSummary.qualsArray(relativePos), false, true, conf)
        else
          agg.quals.updateQuals(currPosition, REF_SYMBOL, readSummary.qualsArray(relativePos), false, true, conf)
      }
      ind += 1
    }
  }
}
