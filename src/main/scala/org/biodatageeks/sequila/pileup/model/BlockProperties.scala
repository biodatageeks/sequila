package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.pileup.model.Alts.SingleLocusAlts
import org.biodatageeks.sequila.pileup.model.Quals.SingleLocusQuals


class BlockProperties {
  var cov, len, pos = 0
  var  alt = new SingleLocusAlts()
  var quals = new SingleLocusQuals()
  def reset (position: Int) = {
    len = 0
    pos = position
  }
  def isNonZeroCoverage:Boolean = {
    len != 0 && cov != 0
  }
  def isZeroCoverage :Boolean ={
    len != 0 && cov == 0
  }
  def isCoverageChanged(newCov:Int, i: Int):Boolean ={
    newCov != 0 && cov >= 0 && cov != newCov && i > 0
  }
  def hasAlt:Boolean ={
    alt.nonEmpty
  }
}
