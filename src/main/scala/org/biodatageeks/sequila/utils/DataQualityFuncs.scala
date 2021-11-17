package org.biodatageeks.sequila.utils

object DataQualityFuncs {
  /**
    * Remove in a case insenstive way chr from the bagining of the rname/contig field
    * @param contig
    * @return
    */
  def cleanContig (contig : String): String = {
    contig match {
      case "chr1" => "1"
      case "chr2" => "2"
      case "chr3" => "3"
      case "chr4" => "4"
      case "chr5" => "5"
      case "chr6" => "6"
      case "chr7" => "7"
      case "chr8" => "8"
      case "chr9" => "9"
      case "chr10" => "10"
      case "chr11" => "11"
      case "chr12" => "12"
      case "chr13" => "13"
      case "chr14" => "14"
      case "chr15" => "15"
      case "chr16" => "16"
      case "chr17" => "17"
      case "chr18" => "18"
      case "chr19" => "19"
      case "chr20" => "20"
      case "chr21" => "21"
      case "chr22" => "22"
      case "chrX" => "X"
      case "chrY" => "Y"
      case "chrM" => "MT"
      case "chrMT" => "MT"
      case "M" => "MT"
      case c if c != null && c.startsWith("chr") => contig.replace("chr","")
      case _ => contig
    }
  }
  def unCleanContig (contig : String): String = {
    contig match {
      case "1" => "chr1"
      case "2" => "chr2"
      case "3" => "chr3"
      case "4" => "chr4"
      case "5" => "chr5"
      case "6" => "chr6"
      case "7" => "chr7"
      case "8" => "chr8"
      case "9" => "chr9"
      case "10" => "chr10"
      case "11" => "chr11"
      case "12" => "chr12"
      case "13" => "chr13"
      case "14" => "chr14"
      case "15" => "chr15"
      case "16" => "chr16"
      case "17" => "chr17"
      case "18" => "chr18"
      case "19" => "chr19"
      case "20" => "chr20"
      case "21" => "chr21"
      case "22" => "chr22"
      case "X" => "chrX"
      case "Y" => "chrY"
      case "MT" => "chrM"
      case c if c != null => "chr"+c
      case _ => contig
    }
  }
}
