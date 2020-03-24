package org.biodatageeks.sequila.pileup.model

/**
  * generic pileup representation
  */
abstract class GenericPileupRecord {

}

/**
  * Simple, fast pileup representation counting number of refs and non-refs
  */
case class PileupRecord (
                          contig: String,
                          pos: Int,
                          ref: String,
                          cov: Short,
                          countRef:Short,
                          countNonRef:Short)
  extends GenericPileupRecord



