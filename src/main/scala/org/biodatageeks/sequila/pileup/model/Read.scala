package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{CigarOperator, SAMRecord}
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.pileup.timers.PileupTimers.{AnalyzeReadsCalculateAltsParseMDTimer, AnalyzeReadsCalculateAltsTimer, AnalyzeReadsCalculateEventsTimer}
import org.biodatageeks.sequila.pileup.timers.PileupTimers._
import org.biodatageeks.sequila.pileup.model.Quals._

import scala.collection.mutable

object ReadOperations {
  object implicits {
    implicit def reads(r:SAMRecord) = ExtendedReads(r)
  }
}

case class ExtendedReads(r:SAMRecord) {

  private def getAltBaseFromSequence(position: Int):Char = this.r.getReadString.charAt(position-1)
  private def getAltBaseQualFromSequence(position: Int):Byte = this.r.getBaseQualities()(position-1)

  def analyzeRead(contig: String,
                  agg: ContigAggregate,
                  contigMaxReadLen: mutable.HashMap[String, Int]
                  ): Unit = {
    val qualityCache = agg.qualityCache

    AnalyzeReadsCalculateEventsTimer.time { calculateEvents(contig, agg, contigMaxReadLen) }
    val foundAlts = AnalyzeReadsCalculateAltsTimer.time{calculateAlts(agg, qualityCache) }
      if (Conf.includeBaseQualities) {
        ReadQualSummaryTimer.time{
          val start = r.getStart
          val cigarConf = CigarDerivedConf.create(start, r.getCigar)
          val readQualSummary = ReadQualSummary(start, r.getEnd, r.getBaseQualities, cigarConf)
          ReadQualSummaryFillExisitingQualTimer.time { fillBaseQualitiesForExistingAlts(agg, foundAlts, readQualSummary) }
          agg.qualityCache.addOrReplace(readQualSummary)
        }
      }
  }

  def calculateEvents(contig: String, aggregate: ContigAggregate, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {
    val partitionStart = aggregate.startPosition
    var position = this.r.getStart
    val cigarIterator = this.r.getCigar.iterator()
    var cigarLen = 0

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator

      if (cigarOperator == CigarOperator.M ||
        cigarOperator == CigarOperator.X   ||
        cigarOperator == CigarOperator.EQ  ||
        cigarOperator == CigarOperator.N   ||
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

  def calculatePositionInReadSeq( mdPosition: Int): Int = {
    val cigar = this.r.getCigar
    if (!cigar.containsOperator(CigarOperator.INSERTION))
      return mdPosition

    var numInsertions = 0
    val cigarIterator = cigar.iterator()
    var position = 0

    while (cigarIterator.hasNext){
      if (position > mdPosition + numInsertions)
        return mdPosition + numInsertions
      val cigarElement = cigarIterator.next()
      val cigarOpLength = cigarElement.getLength
      val cigarOp = cigarElement.getOperator

      if (cigarOp == CigarOperator.INSERTION) {
        numInsertions += cigarOpLength
      }
      position = position+cigarOpLength
    }
    mdPosition + numInsertions
  }

  private def calculateAlts(aggregate: ContigAggregate, qualityCache: QualityCache): scala.collection.Set[Int] = {
    val read = this.r
    var position = read.getStart
    val md = read.getAttribute("MD").toString
    val ops = AnalyzeReadsCalculateAltsParseMDTimer.time { MDTagParser.parseMDTag(md) }
    var delCounter = 0
    var altsPositions = mutable.Set.empty[Int]
    val clipLen =
      if (read.getCigar.getCigarElement(0).getOperator.isClipping)
        read.getCigar.getCigarElement(0).getLength else 0

    position += clipLen

    for (mdtag <- ops) {
      if (mdtag.isDeletion) {
        delCounter += 1
        position +=1
      } else if (mdtag.base != 'S') {
        position += 1

        val indexInSeq = calculatePositionInReadSeq(position - read.getStart -delCounter)
        val altBase = read.getReadString.charAt(indexInSeq-1)
        val altBaseQual = read.getBaseQualities()(indexInSeq-1)
        val altPosition = position - clipLen - 1
        aggregate.updateAlts(altPosition, altBase)
        if(Conf.includeBaseQualities) {
          if (!aggregate.alts.contains(altPosition))
            fillPastQualitiesFromCache(aggregate, altPosition, altBase, altBaseQual, qualityCache)
          else
            aggregate.updateQuals(altPosition, altBase, altBaseQual, true)
        }
        altsPositions+=altPosition
      }
      else if(mdtag.base == 'S')
        position += mdtag.length
    }
    altsPositions
  }

  private def updateMaxCigarInContig(cigarLen:Int, contig: String, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {
    if (cigarLen > contigMaxReadLen(contig))
      contigMaxReadLen(contig) = cigarLen
  }


  def fillBaseQualitiesForExistingAlts(agg: ContigAggregate, blackList:scala.collection.Set[Int], readQualSummary: ReadQualSummary): Unit = {
    val positionsToFill =  (agg.altsKeyCache.range(r.getStart,r.getEnd+1) diff blackList).toArray
    var ind = 0
    while (ind < positionsToFill.length) {
      val altPosition = positionsToFill(ind)
      if(!readQualSummary.cigarDerivedConf.hasDel || !readQualSummary.hasDeletionOnPosition(altPosition) ) {
        val relativePos = if(!readQualSummary.cigarDerivedConf.hasIndel && !readQualSummary.cigarDerivedConf.hasClip ) altPosition - readQualSummary.start
        else readQualSummary.relativePosition(altPosition)
        agg.quals.updateQuals(altPosition, QualityConstants.REF_SYMBOL,readQualSummary.qualsArray(relativePos), false, true)
      }
      ind += 1
    }
  }

  def fillPastQualitiesFromCache(agg: ContigAggregate, altPosition: Int, altBase:Char, altBaseQual: Byte, qualityCache: QualityCache): Unit = {
    val reads = qualityCache.getReadsOverlappingPosition(altPosition)
    val altQualArr = new Array [Short] (Conf.qualityArrayLength)
    val locusQuals = new SingleLocusQuals(QualityConstants.OUTER_QUAL_SIZE)
    agg.quals(altPosition) = locusQuals
    locusQuals(altBase - QualityConstants.QUAL_INDEX_SHIFT) = altQualArr

    altQualArr(altBaseQual) = 1
    altQualArr(Conf.qualityArrayLength - 1 ) = altBaseQual

    if(reads.isEmpty)
      return

    val refQualArr = new Array [Short] (Conf.qualityArrayLength)
    var maxQual = 0.toShort

    var ind = 0
     while (ind < reads.length) {
       val readQualSummary = reads(ind)
       val relativePos = if(!readQualSummary.cigarDerivedConf.hasIndel && !readQualSummary.cigarDerivedConf.hasClip ) altPosition - readQualSummary.start
       else readQualSummary.relativePosition(altPosition)
       val qual = readQualSummary.qualsArray(relativePos)
       refQualArr(qual) = (refQualArr(qual) + 1).toShort
       if (qual > maxQual)
         maxQual = qual
       ind += 1
     }
    refQualArr(Conf.qualityArrayLength-1) = maxQual
    locusQuals(QualityConstants.REF_SYMBOL - QualityConstants.QUAL_INDEX_SHIFT) = refQualArr
  }
}
