package org.biodatageeks.sequila.pileup.converters.common

object PileupFormats {

  val SAMTOOLS_FORMAT  = "samtools"
  val SAMTOOLS_FORMAT_SHORT = "sam"

  val GATK_FORMAT = "gatk"

  val SEQUILA_FORMAT = "sequila"

  def isSamtools (format: String): Boolean =  {
    format == SAMTOOLS_FORMAT || format == SAMTOOLS_FORMAT_SHORT
  }

  def isGatk (format: String): Boolean = {
    format == GATK_FORMAT
  }

  def isSequila (format: String): Boolean = {
    format == SEQUILA_FORMAT
  }

  case class isSamtools(format: String) {

  }

}
