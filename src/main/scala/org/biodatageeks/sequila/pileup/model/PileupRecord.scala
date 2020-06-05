package org.biodatageeks.sequila.pileup.model

import scala.collection.mutable

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
                          start: Int,
                          end: Int,
                          ref: String,
                          cov: Short,
                          countRef:Short,
                          countNonRef:Short,
                          alts: Map[Byte, Short])
  extends GenericPileupRecord



