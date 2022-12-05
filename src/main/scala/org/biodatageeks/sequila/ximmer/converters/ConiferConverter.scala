package org.biodatageeks.sequila.ximmer.converters

import java.io.{File, PrintWriter}
import scala.io.Source

class ConiferConverter {

  def convertToConiferFormat(sample: String, targetCountFile: String, bamInfoFile: String, outputPath: String) : Unit = {
    val readsNumber = readTotalReadsNumber(bamInfoFile)
    var iterator = 1
    val fileObject = new File(outputPath + "/" + sample + ".rpkm")
    val pw = new PrintWriter(fileObject)

    val content = Source.fromFile(targetCountFile)
    val lines = content.getLines()

    for (line <- lines) {
      val elements = line.split(",")
      val targetStart = elements(1).toInt
      val targetEnd = elements(2).toInt
      val cov = elements(6).toInt
      val exonLength = targetEnd - targetStart
      val rpkm = BigDecimal(calculateRpkm(cov, exonLength, readsNumber)).setScale(6, BigDecimal.RoundingMode.FLOOR).toDouble
      pw.write(iterator + "\t" + cov + "\t" + rpkm + "\n")
      iterator += 1
    }
    pw.close()
    content.close()
  }

  private def readTotalReadsNumber(bamInfoFile: String) : Int = {
    val content = Source.fromFile(bamInfoFile)
    val totalReadsNumber = content.getLines().next().toInt
    content.close()
    totalReadsNumber
  }

  private def calculateRpkm(readCount: Int, exonLength: Int, totalReads: Int) : Double = {
    (scala.math.pow(10, 9) * readCount / exonLength) / totalReads
  }

}
