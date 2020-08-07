package org.biodatageeks.sequila.pileup.model

import java.io.File

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import org.biodatageeks.sequila.utils.DataQualityFuncs

import scala.collection.mutable

class Reference(refPath : String) {

  val fasta = new IndexedFastaSequenceFile(new File(refPath))

  def getNormalizedContigMap: mutable.HashMap[String, String] = {
    val normContigMap = new mutable.HashMap[String, String]()
    val iter = fasta.getIndex.iterator()
    while (iter.hasNext){
      val contig = iter.next().getContig
      normContigMap += (DataQualityFuncs.cleanContig(contig) -> contig )
    }
    normContigMap
  }
  def getBaseFromReference(contigMap: mutable.HashMap[String,String], contig: String, index: Int): String = {
    val refBase = fasta.getSubsequenceAt(contigMap(contig), index.toLong, index.toLong)
    refBase.getBaseString.toUpperCase
  }

  def getBasesFromReference(contig: String, startIndex: Int, endIndex: Int): String = {
    val refBases = fasta.getSubsequenceAt(contig, startIndex.toLong, endIndex.toLong-1)
    refBases.getBaseString.toUpperCase
  }
}
