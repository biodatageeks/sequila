package org.biodatageeks.sequila.utils

object DataQualityFuncs {
  /**
    * Remove in a case insenstive way chr from the bagining of the rname/contig field
    * @param contig
    * @return
    */
  def cleanContig (contig : String) = {
    if(contig != null)
    contig
      .replaceFirst("^(?i)chrM$", "chrMT")
      .replaceFirst("^(?i)chr","")
      .replaceFirst("^M$", "MT")
    else
      contig
  }
}
