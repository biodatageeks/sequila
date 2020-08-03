package org.biodatageeks.sequila.pileup.conf

object QualityConstants {
  final val REF_SYMBOL = 'R'
  final val FREQ_QUAL= 'F'.toShort
  final val CACHE_EXPANDER=2
  final val CACHE_SIZE = 600
  final val MAX_QUAL = 93
  final val QUAL_ARR_SIZE = MAX_QUAL + 2
  final val OFFSET  = 33
  final val MAX_QUAL_IND = MAX_QUAL + 1

}
