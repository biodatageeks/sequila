package org.biodatageeks.sequila.utils

import org.biodatageeks.formats.Alignment


object Columns {

  /*BAM/CRAM Fields*/
  private val alignmentColumns = ScalaFuncs
    .classAccessors[Alignment]
    .map(_.name.toString)

  final val SAMPLE = alignmentColumns(0)
  final val QNAME = alignmentColumns(1)
  final val FLAGS = alignmentColumns(2)
  final val CONTIG = alignmentColumns(3)
  final val POS = alignmentColumns(4)
  final val START = alignmentColumns(5)
  final val END = alignmentColumns(6)
  final val MAPQ = alignmentColumns(7)
  final val CIGAR = alignmentColumns(8)
  final val RNEXT = alignmentColumns(9)
  final val PNEXT = alignmentColumns(10)
  final val TLEN =  alignmentColumns(11)
  final val SEQUENCE = alignmentColumns(12)
  final val BASEQ = alignmentColumns(13)
  final val TAG = alignmentColumns(14)
  final val MATEREFIND = "materefind"
  final val SAMRECORD = "SAMRecord"
  final val STRAND = "strand"
  final val COVERAGE= "coverage"

}
