package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.util.StringUtil
import htsjdk.samtools.{CigarOperator, SAMRecord}
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.pileup.timers.PileupTimers.{AnalyzeReadsCalculateAltsParseMDTimer, AnalyzeReadsCalculateAltsTimer, AnalyzeReadsCalculateEventsTimer}
import org.biodatageeks.sequila.pileup.timers.PileupTimers._

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
          val cigar = r.getCigar
          val start = r.getStart
          val cigarConf = CigarDerivedConf.create(start, cigar)
          val readQualSummary = ReadQualSummary(start, r.getEnd, r.getBaseQualities, cigarConf)
          ReadQualSummaryFillExisitingQualTimer.time { fillBaseQualitiesForExistingAlts(agg, foundAlts, readQualSummary) }
          agg.addToCache(readQualSummary)
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
        val altBase = getAltBaseFromSequence(indexInSeq)
        val altBaseQual = getAltBaseQualFromSequence(indexInSeq)
        val altPosition = position - clipLen - 1
        val newAlt = !aggregate.hasAltOnPosition(altPosition)
        aggregate.updateAlts(altPosition, altBase)
        if(Conf.includeBaseQualities) aggregate.updateQuals(altPosition, altBase, altBaseQual, true)

        if (newAlt && Conf.includeBaseQualities)
          fillPastQualitiesFromCache(aggregate, altPosition, qualityCache)
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


  //~100s
  def fillBaseQualitiesForExistingAlts(agg: ContigAggregate, blackList:scala.collection.Set[Int], readQualSummary: ReadQualSummary): Unit = {
    val altsPositions =  agg.getAltPositionsForRange(r.getStart, r.getEnd) //~1s
    val positionsToFill =  altsPositions diff blackList //~1s
    for (pos <- positionsToFill.iterator) { //~10s empty loop
      if(!readQualSummary.cigarDerivedConf.hasDel || !readQualSummary.hasDeletionOnPosition(pos) ) {
        val relativePos = if(!readQualSummary.cigarDerivedConf.hasIndel && !readQualSummary.cigarDerivedConf.hasClip ) pos - readQualSummary.start
        else readQualSummary.relativePosition(pos)
        val qual = readQualSummary.qualsArray(relativePos)
        agg.updateQuals(pos, QualityConstants.REF_SYMBOL, qual, false)
      }
    }
  }

  def fillPastQualitiesFromCache(agg: ContigAggregate, altPosition: Int, qualityCache: QualityCache): Unit = {
    val reads = qualityCache.getReadsOverlappingPosition(altPosition)
     for (read <- reads) {
       val relativePos = if(!read.cigarDerivedConf.hasIndel && !read.cigarDerivedConf.hasClip ) altPosition - read.start
       else read.relativePosition(altPosition)
       agg.updateQuals(altPosition, QualityConstants.REF_SYMBOL, read.qualsArray(relativePos) )
     }
  }
}
