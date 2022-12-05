package org.biodatageeks.sequila.ximmer.converters

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

class ExomeDepthConverter {

  val recordsByChr: mutable.Map[String, ListBuffer[Record]] = mutable.LinkedHashMap[String, ListBuffer[Record]]()

  def convertToExomeDepthFormat(sampleFiles: List[String], sampleNames: List[String], outputPath: String): Unit = {
    val header = List(addExtraQuotes("chromosome"), addExtraQuotes("start"), addExtraQuotes("end"), addExtraQuotes("exon"))
      .mkString(" ") + " " +
      sampleNames.toStream
        .map(x => addExtraQuotes(x))
        .toList
        .mkString(" ")

    for (sampleFile <- sampleFiles) {
      fillCoveragesForSample(sampleFile)
    }

    recordsByChr.foreach(x => {
      val chr = x._1
      val regions = x._2
      val fileName = "analysis." + chr + ".counts.tsv"
      val fileObject = new File(outputPath + "/" + fileName)
      val pw = new PrintWriter(fileObject)
      pw.write(header)
      pw.write("\n")

      regions.foreach(r => {
        pw.write(r.toString())
        pw.write("\n")
      })
      pw.close()
    })
  }

  private def addExtraQuotes(s: String): String = {
    "\"" + s + "\""
  }

  private def fillCoveragesForSample(sampleFile: String): Unit = {
    val content = Source.fromFile(sampleFile)
    val lines = content.getLines()
    for (line <- lines) {
      val elements = line.split(",")
      val record = recordFromElements(elements)
      val cov = elements(5)
      val chr = record.chr

      if (!recordsByChr.contains(chr)) {
        recordsByChr += (chr -> new ListBuffer[Record])
      }
      if (!recordsByChr(chr).contains(record)) {
        recordsByChr(chr) += record
      }

      recordsByChr(chr)
        .find(x => x.equals(record))
        .get
        .addCov(cov)
    }

    content.close()
  }

  private def recordFromElements(elements: Array[String]): Record = {
    val chr = elements(0)
    val start = elements(1).toInt
    val end = elements(2).toInt - 1
    val exon = "\"" + chr + "-" + start + "-" + end + "\""
    new Record(chr, (start + 1).toString, end.toString, exon)
  }

  class Record(val chr: String, val start: String, val end: String, val exon: String) {
    private val covs = new ListBuffer[String]()

    def addCov(value: String): Unit = {
      covs += value
    }

    override def toString(): String = {
      var objectList = List(addExtraQuotes(chr), start, end, exon)
      objectList ++= covs.toList
      objectList.mkString(" ")
    }

    override def equals(o: Any): Boolean = {
      if (!o.isInstanceOf[Record]) return false
      val other = o.asInstanceOf[Record]

      (this.chr == other.chr) && (this.start == other.start) && (this.end == other.end) && (this.exon == other.exon)
    }

    override def hashCode(): Int = {
      val PRIME = 59
      exon.hashCode() * PRIME
    }
  }

}
