package org.biodatageeks.sequila.pileup

import java.io.File

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import org.biodatageeks.sequila.utils.DataQualityFuncs

import scala.collection.mutable

object Reference {
  val s = ""

//  def getBasesFromReference(contig: String, startIndex: Int, endIndex: Int): String = {
//    val fasta = new IndexedFastaSequenceFile(new File(path))
//    val normContig =
//    val refBases = fasta.getSubsequenceAt(contig, startIndex.toLong, endIndex.toLong-1)
//    refBases.getBaseString.toUpperCase
//  }
//
//  def getNormalizedContigMap(fasta: IndexedFastaSequenceFile): mutable.Map[String, String] = {
//    val normContigMap = new mutable.HashMap[String, String]()
//    val iter = fasta.getIndex.iterator()
//    while (iter.hasNext){
//      val contig = iter.next().getContig
//      normContigMap += (DataQualityFuncs.cleanContig(contig) -> contig )
//    }
//    normContigMap
//  }
}

class Reference (path:String) {

  private def doSth () = {

  }
}

