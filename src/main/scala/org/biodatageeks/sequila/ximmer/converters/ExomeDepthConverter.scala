package org.biodatageeks.sequila.ximmer.converters

import org.apache.spark.sql.{DataFrame, Row}

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ExomeDepthConverter {

  val recordsByChr: mutable.Map[String, ListBuffer[ListBuffer[String]]] = mutable.LinkedHashMap[String, ListBuffer[ListBuffer[String]]]()

  def convertToExomeDepthFormat(targetCountResult: mutable.Map[String, (DataFrame, Long)], outputPath: String): Unit = {
    val sampleNames = targetCountResult.keys.toList
    val header = List(addExtraQuotes("chromosome"), addExtraQuotes("start"), addExtraQuotes("end"), addExtraQuotes("exon"))
      .mkString(" ") + " " +
      sampleNames.toStream
        .map(x => addExtraQuotes(x))
        .toList
        .mkString(" ")

    val sampleList = targetCountResult.map(x => x._2._1).toList
    var beforeInit = true
    for (sampleDF <- sampleList) {
      val collected = sampleDF.collect()
      if (beforeInit) {
        initTargets(collected)
        beforeInit = false
      }

      fillCoveragesForSample(collected)
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
        pw.write(r.mkString(" "))
        pw.write("\n")
      })
      pw.close()
    })
  }

  private def addExtraQuotes(s: String): String = {
    "\"" + s + "\""
  }

  private def initTargets(rows: Array[Row]): Unit = {
    rows.foreach(row => {
      val chr = row.getString(0)
      val start = row.getString(1).toInt
      val end = row.getString(2).toInt - 1
      val exon = "\"" + chr + "-" + start + "-" + end + "\""
      val record = ListBuffer(addExtraQuotes(chr), (start + 1).toString, end.toString, exon)
      if (!recordsByChr.contains(chr)) {
        recordsByChr += (chr -> new ListBuffer[ListBuffer[String]])
      }
      recordsByChr(chr) += record
    })
  }

  private def fillCoveragesForSample(rows: Array[Row]): Unit = {
    val chrList = recordsByChr.keys.toList
    val iteratorByChr = mutable.LinkedHashMap[String, Int]()
    for (chr <- chrList) {
      iteratorByChr += (chr -> 0)
    }

    rows.foreach(row => {
      val chr = row.getString(0)
      val cov = row.getLong(5)
      val iterator = iteratorByChr(chr)
      recordsByChr(chr)(iterator) += cov.toString
      iteratorByChr(chr) += 1
    })
  }

}
