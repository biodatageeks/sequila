package org.biodatageeks.sequila.ximmer.converters

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.utils.InternalParams

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CodexConverter {
  val mergedFormatJson: String =
    s"""{
       |  "type": "integer",
       |  "attributes": {
       |    "dim": {
       |      "type": "integer",
       |      "attributes": {},
       |      "value": [%s, %s]
       |    }
       |  },
       |  "value": %s
       |}""".stripMargin

  var coveragesByChrMap: mutable.Map[String, ListBuffer[Int]] = mutable.Map[String, ListBuffer[Int]]()

  def convertToCodexFormat(targetCountResult: mutable.Map[String, (DataFrame, Long)], outputPath: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val samplesCount = targetCountResult.size

    for (targetCountResult <- targetCountResult) {
      readAndFillCoveragesByChrMap(targetCountResult._2._1)
    }

    for ((chr, values) <- coveragesByChrMap) {
      val fileName = "analysis.codex_coverage." + chr + ".json"
      val fileObject = new File(outputPath + "/" + fileName)
      val pw = new PrintWriter(fileObject)

      val stringValue = mergedFormatJson.format(
        values.size / samplesCount,
        samplesCount,
        "[" + values.toList.mkString(", ") + "]"
      )

      pw.write(stringValue)
      pw.close()

      if (spark.conf.get(InternalParams.saveAsSparkFormat).toBoolean) {
        val resultDF = spark.read.option("wholetext", value = true).text(outputPath + "/" + fileName)
        resultDF.write.text(outputPath + "/spark" + chr)
      }
    }
  }

  private def readAndFillCoveragesByChrMap(targetCount: DataFrame): Unit = {
    targetCount.collect().foreach(row => {
      val chr = row.getString(0)
      val cov = row.getLong(3).toInt
      if (!coveragesByChrMap.contains(chr)) {
        coveragesByChrMap += (chr -> new ListBuffer[Int])
      }
      coveragesByChrMap(chr) += cov
    })
  }

}
