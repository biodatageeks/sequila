package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator, SAMRecord}
import org.apache.spark.broadcast.Broadcast
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.conf.QualityConstants.{OUTER_QUAL_SIZE, QUAL_INDEX_SHIFT, REF_SYMBOL}
import org.biodatageeks.sequila.pileup.model.Quals._
import org.biodatageeks.sequila.pileup.model.Alts._

import scala.collection.mutable

object ReadOperations {

  object implicits {
    implicit def reads(r: SAMRecord) = ExtendedReads(r)
  }

}

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
    val foundAlts = calculateAlts(agg, agg.qualityCache, start, cigar, bQual, isPositiveStrand, conf)

    if (conf.value.includeBaseQualities) {
      val cigarConf = CigarDerivedConf.create(start, cigar)
      val readQualSummary = ReadQualSummary(start, read.getEnd, read.getBaseQualities, cigarConf)
      fillBaseQualitiesForExistingAlts(agg, foundAlts, readQualSummary, conf)
      agg.qualityCache.addOrReplace(readQualSummary)
    }
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

  def calculateAlts(aggregate: ContigAggregate, qualityCache: QualityCache, start: Int,
                    cigar: Cigar, bQual: Array[Byte], 
                    isPositiveStrand:Boolean, conf: Broadcast[Conf]): scala.collection.Set[Int] = {
    var position = start
    val ops = MDTagParser.parseMDTag(read.getAttribute("MD").toString)

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

        if (conf.value.includeBaseQualities) {
          if (!aggregate.alts.contains(altPosition))
            fillPastQualitiesFromCache(aggregate, altPosition, altBase, altBaseQual, qualityCache, conf)
          else
            aggregate.quals.updateQuals(altPosition, altBase, altBaseQual, firstUpdate = true, updateMax = true, conf)
        }
        aggregate.alts.updateAlts(altPosition, altBase)
        if (conf.value.includeBaseQualities)
          aggregate.altsKeyCache.add(altPosition)
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


  def fillBaseQualitiesForExistingAlts(agg: ContigAggregate, blackList: scala.collection.Set[Int],
                                       readQualSummary: ReadQualSummary,
                                       conf: Broadcast[Conf]): Unit = {
    val positionsToFill = (agg.altsKeyCache.range(readQualSummary.start, readQualSummary.end + 1) diff blackList).toArray
    var ind = 0
    while (ind < positionsToFill.length) {
      val altPosition = positionsToFill(ind)
      if (!readQualSummary.cigarDerivedConf.hasDel || !readQualSummary.hasDeletionOnPosition(altPosition)) {
        val relativePos = if (!readQualSummary.cigarDerivedConf.hasIndel && !readQualSummary.cigarDerivedConf.hasClip) altPosition - readQualSummary.start
        else readQualSummary.relativePosition(altPosition)
        agg.quals.updateQuals(altPosition, REF_SYMBOL, readQualSummary.qualsArray(relativePos), false, true, conf)
      }
      ind += 1
    }
  }

  def fillPastQualitiesFromCache(agg: ContigAggregate, altPosition: Int, altBase: Char,
                                 altBaseQual: Byte, qualityCache: QualityCache,
                                 conf: Broadcast[Conf]): Unit = {

    val reads = qualityCache.getReadsOverlappingPosition(altPosition)
    val altQualArr = new Array[Short](conf.value.qualityArrayLength)
    val locusQuals = new SingleLocusQuals(OUTER_QUAL_SIZE)
    agg.quals(altPosition) = locusQuals
    locusQuals(altBase - QUAL_INDEX_SHIFT) = altQualArr

    altQualArr(altBaseQual) = 1
    altQualArr(conf.value.qualityArrayLength - 1) = altBaseQual

    if (reads.isEmpty)
      return

    val refQualArr = new Array[Short](conf.value.qualityArrayLength)
    var maxQual = 0.toShort

    var ind = 0
     while (ind < reads.length) {
       val readQualSummary = reads(ind)
       val relativePos = if(!readQualSummary.cigarDerivedConf.hasIndel && !readQualSummary.cigarDerivedConf.hasClip ) altPosition - readQualSummary.start
       else readQualSummary.relativePosition(altPosition)
       val qual =  if (conf.value.isBinningEnabled) (readQualSummary.qualsArray(relativePos)/conf.value.binSize).toByte  else readQualSummary.qualsArray(relativePos)
       refQualArr(qual) = (refQualArr(qual) + 1).toShort
       if (qual > maxQual)
         maxQual = qual
       ind += 1
     }
    refQualArr(conf.value.qualityArrayLength-1) = maxQual
    locusQuals(REF_SYMBOL - QUAL_INDEX_SHIFT) = refQualArr
  }
}
