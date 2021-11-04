package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator, SAMRecord}
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.pileup.conf.QualityConstants
import org.biodatageeks.sequila.pileup.model.Quals._
import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeRedBlack

import scala.collection.mutable
import collection.JavaConverters._
object ReadOperations {

  object implicits {
    implicit def reads(r: SAMRecord) = ExtendedReads(r)
  }
}
case class TruncRead(rName: String, contig: String, posStart: Int, posEnd: Int)
case class ExtendedReads(read: SAMRecord) {


  def addReadToQualsBuffer(readSummaryTree: IntervalTreeRedBlack[ReadSummary]): ReadSummary = {
    val cigarConf = CigarDerivedConf.create(read.getStart, read.getCigar)
    val readQualSummary = ReadSummary(read.getStart, read.getEnd, read.getReadBases, read.getBaseQualities, ! read.getReadNegativeStrandFlag, cigarConf)
    readSummaryTree.put(read.getStart, read.getEnd, readQualSummary)
  }

  def analyzeRead(agg: ContigAggregate,
                  qualsWindowProcessWatermark: Int,
                  readSummaryTree: IntervalTreeRedBlack[ReadSummary],
                  altsTree : IntervalTreeRedBlack[Int]): Int = {

    calculateEvents(agg)
    calculateAlts(agg, altsTree)

    if (agg.conf.includeBaseQualities) {
      addReadToQualsBuffer (readSummaryTree)
      processQualsBuffer (agg, readSummaryTree, altsTree, qualsWindowProcessWatermark)
    }
    else
      qualsWindowProcessWatermark
  }

  def processQualsBuffer(agg: ContigAggregate,
                         readSummaryTree: IntervalTreeRedBlack[ReadSummary],
                         altsTree: IntervalTreeRedBlack[Int],
                         qualsWindowProcessWatermark: Int
                    ):Int = {
    val qualsWindowPos = read.getStart
    if (qualsWindowPos <=  qualsWindowProcessWatermark)
      return qualsWindowProcessWatermark
    val windowStart = qualsWindowProcessWatermark - (QualityConstants.PROCESS_SIZE + 1)
    val windowEnd = read.getStart - 1
    flushQualsBuffer(readSummaryTree, altsTree, windowStart, windowEnd, agg)
    read.getStart + QualityConstants.PROCESS_SIZE + 1
  }

  def flushQualsBuffer(readSummaryTree: IntervalTreeRedBlack[ReadSummary], altsTree: IntervalTreeRedBlack[Int], start: Int, end: Int, agg:ContigAggregate): Unit = {
    val altsArray = altsTree.overlappers(start, end).asScala.flatMap(r=>r.getValue.asScala).toArray.distinct
    if(altsArray.length > 0) {
      val rsIterator = readSummaryTree.overlappers(start, end)
      while (rsIterator.hasNext) {
        val nodeIterator = rsIterator.next().getValue.iterator()
        while (nodeIterator.hasNext) {
          fillBaseQualities(agg,  nodeIterator.next(), altsArray)
        }
      }
    }
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

  def calculateAlts(aggregate: ContigAggregate,
                    altsTree: IntervalTreeRedBlack[Int]): Unit = {
    val start = read.getStart
    val cigar = read.getCigar
    val isPositiveStrand = !read.getReadNegativeStrandFlag
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
        if(aggregate.conf.includeBaseQualities)
          altsTree.put(altPosition, altPosition, altPosition)

      }
      else if (mdtag.base == 'S')
        position += mdtag.length
    }
  }

  def fillBaseQualities(agg: ContigAggregate, readSummary: ReadSummary, altsArray: Array[Int]): Unit = {
    var idx = 0
    while (idx < altsArray.length) {
      val currPosition = altsArray(idx)
      if (currPosition >= readSummary.start && currPosition<= readSummary.end && !readSummary.hasDeletionOnPosition(currPosition)) {
        val relativePos = if (!readSummary.cigarDerivedConf.hasIndel && !readSummary.cigarDerivedConf.hasClip) currPosition - readSummary.start
        else readSummary.relativePosition(currPosition)
        if (agg.contig == "1" && currPosition == 3092)
          println()
          val base = if(readSummary.isPositiveStrand)  readSummary.basesArray(relativePos).toChar.toUpper else readSummary.basesArray(relativePos).toChar.toLower
          agg.quals.updateQuals(currPosition, base, readSummary.qualsArray(relativePos), agg.conf)
      }
        idx += 1
    }
  }
}
