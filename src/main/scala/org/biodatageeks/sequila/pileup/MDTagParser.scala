package org.biodatageeks.sequila.pileup

import java.io.File

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.samtools.{Cigar, CigarOperator, SAMRecord}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class MDOperator(length: Int, base: Char) { //S means to skip n positions, not fix needed
  def isDeletion:Boolean = base.isLower
  def isNonDeletion:Boolean = base.isUpper
}
object MDTagParser{

  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)
  val pattern = "([0-9]+)\\^?([A-Za-z]+)?".r

  def parseMDTag(t : String) = {

    if (isAllDigits(t)) {
      Array[MDOperator](MDOperator(t.toInt, 'S'))
    }
    else {
      val ab = new ArrayBuffer[MDOperator]()
      val matches = pattern
        .findAllIn(t)
      while (matches.hasNext) {
        val m = matches.next()
        if(m.last.isLetter && !m.contains('^') ){
          val skipPos = m.dropRight(1).toInt
          ab.append(MDOperator(skipPos, 'S') )
          ab.append(MDOperator(0, m.last.toUpper))
        }
        else if (m.last.isLetter && m.contains('^') ){ //encoding deletions as lowercase
          val arr =  m.split('^')
          val skipPos = arr.head.toInt
          ab.append(MDOperator(skipPos, 'S') )
          arr(1).foreach { b =>
            ab.append(MDOperator(0, b.toLower))
          }
        }
        else ab.append(MDOperator(m.toInt, 'S') )
      }
      ab.toArray
    }
  }


  private def isAllDigits(s: String) : Boolean = {
    val len = s.length
    var i = 0
      while(i < len){
        if(! s(i).isDigit ) return false
        i += 1
      }
    true
  }

}
