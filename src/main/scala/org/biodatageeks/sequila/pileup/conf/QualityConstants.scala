package org.biodatageeks.sequila.pileup.conf

object QualityConstants {
  final val REF_SYMBOL = 'R'
  final val FREQ_QUAL= 'F'.toShort
  final val CACHE_EXPANDER = 2
  final val CACHE_SIZE = 600
  final val DEFAULT_MAX_QUAL = 40
  final val DEFAULT_BIN_SIZE = 1
  final val QUAL_INDEX_SHIFT = 'A'.toInt
  final val OUTER_QUAL_SIZE = 't' - 'A' + 1
}
