package org.biodatageeks.sequila.utils

import org.biodatageeks.formats.{Alignment, BrowserExtensibleData, SequencedFragment}


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
  final val COUNT_REF="countRef"
  final val COUNT_NONREF="countNonRef"
  final val QUALS="quals"

  private val sequencedFragmentColumns = ScalaFuncs
    .classAccessors[SequencedFragment]
    .map(_.name.toString)

  final val INSTRUMENT_NAME = sequencedFragmentColumns(1)
  final val RUN_ID = sequencedFragmentColumns(2)
  final val FLOWCELL_ID = sequencedFragmentColumns(3)
  final val FLOWCELL_LANE = sequencedFragmentColumns(4)
  final val TILE = sequencedFragmentColumns(5)
  final val X_POS = sequencedFragmentColumns(6)
  final val Y_POS = sequencedFragmentColumns(7)
  final val FILTER_PASSED = sequencedFragmentColumns(8)
  final val CONTROL_NUMBER = sequencedFragmentColumns(9)
  final val INDEX_SEQUENCE = sequencedFragmentColumns(10)


  private val BEDColumns = ScalaFuncs
    .classAccessors[BrowserExtensibleData]
    .map(_.name.toString)

  final val NAME = BEDColumns(3)
  final val SCORE = BEDColumns(4)
  final val THICK_START = BEDColumns(6)
  final val THICK_END = BEDColumns(7)
  final val ITEM_RGB = BEDColumns(8)
  final val BLOCK_COUNT = BEDColumns(9)
  final val BLOCK_SIZES = BEDColumns(10)
  final val BLOCK_STARTS = BEDColumns(11)


  final val REF = "ref"
  final val ALT = "alt"
  final val ALTS= "alts"
}


