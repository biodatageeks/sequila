package org.biodatageeks.sequila.ximmer.converters

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

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

  var coveragesByChrMap : mutable.Map[String, ListBuffer[Int]] = mutable.Map[String, ListBuffer[Int]]()

  def convertToCodexFormat(targetCountFiles: List[String], outputPath: String): Unit = {
    val samplesCount = targetCountFiles.size

    for (targetCountFile <- targetCountFiles) {
      readAndFillCoveragesByChrMap(targetCountFile)
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
    }
  }

  private def readAndFillCoveragesByChrMap(targetCountFile: String): Unit = {
    val content = Source.fromFile(targetCountFile)
    val lines = content.getLines()
    for (line <- lines) {
      val elements = line.split(",")
      val chr = elements(0)
      val cov = elements(3)
      if (!coveragesByChrMap.contains(chr)) {
        coveragesByChrMap += (chr -> new ListBuffer[Int])
      }
      coveragesByChrMap(chr) += cov.toInt
    }

    content.close()
  }

}
