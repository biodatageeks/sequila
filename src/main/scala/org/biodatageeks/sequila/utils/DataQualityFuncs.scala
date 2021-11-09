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
      case c if c !=null && c.startsWith("chr") => contig.replace("chr","")
      case _ => contig
    }
  }

}
