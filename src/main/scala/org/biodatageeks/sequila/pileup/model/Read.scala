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


  def analyzeRead(agg: ContigAggregate): Unit = {

    calculateEvents(agg)

    if (agg.conf.coverageOnly)
      return
    val rs = ReadSummary(read.getStart, read.getEnd, read.getReadBases, read.getBaseQualities, ! read.getReadNegativeStrandFlag, read.getCigar)
    calculateAlts(agg, rs)
    agg.addReadToBuffer(rs)

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

  def calculateAlts(aggregate: ContigAggregate, rs: ReadSummary): Unit = {
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

        val indexInSeq = rs.calculatePositionInReadSeq(position - start - delCounter)

        val altBase = if (isPositiveStrand) rs.bases(indexInSeq - 1).toChar else rs.bases(indexInSeq - 1).toChar.toLower
        val altPosition = position - clipLen - 1

        aggregate.alts.update(altPosition, altBase)
      }
      else if (mdtag.base == 'S')
        position += mdtag.length
    }
  }
}
