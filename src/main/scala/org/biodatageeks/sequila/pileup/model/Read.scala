package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{CigarOperator, SAMRecord}
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
    val bases = read.getReadBases
    calculateEvents(agg)
    if (agg.conf.coverageOnly)
      return

    val rs = ReadSummary(read.getStart, read.getEnd, bases, read.getBaseQualities, ! read.getReadNegativeStrandFlag, read.getCigar)
    calculateAlts(agg, rs)
    if (agg.conf.includeBaseQualities)
      agg.addReadToBuffer(rs)
  }

  def calculateEvents(agg: ContigAggregate): Unit = {
    val cigar = read.getCigar
    val partitionStart = agg.startPosition
    var position = read.getStart
    val cigarIterator = cigar.iterator()

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator

      if (cigarOperator == CigarOperator.M ||
        cigarOperator == CigarOperator.X ||
        cigarOperator == CigarOperator.EQ ||
        cigarOperator == CigarOperator.N ||
        cigarOperator == CigarOperator.D)

      // update events array according to read alignment blocks start/end
      if (cigarOperator == CigarOperator.M || cigarOperator == CigarOperator.X || cigarOperator == CigarOperator.EQ) {

        agg.updateEvents(position, partitionStart, delta = 1)
        position += cigarOperatorLen
        agg.updateEvents(position, partitionStart, delta = -1)
      }
      else if (cigarOperator == CigarOperator.N || cigarOperator == CigarOperator.D)
        position += cigarOperatorLen
    }
  }

  def calculateAlts(agg: ContigAggregate, rs: ReadSummary): Unit = {
    var position = rs.start
    val ops = MDTagParser.parseMDTag(read.getStringAttribute("MD"))

    for (mdtag <- ops) {
      if (mdtag.isDeletion)
        position += 1
      else if (mdtag.base != 'S') {
        agg.alts.update(position, rs.getBaseForAbsPosition(position))
        position += 1
      } else if (mdtag.base == 'S')
        position += mdtag.length
    }
  }
}
