package org.biodatageeks.sequila.ximmer.converters

import org.apache.spark.sql.DataFrame

import java.io.{File, PrintWriter}

class ConiferConverter {

  def convertToConiferFormat(targetCountResult: (String, (DataFrame, Long)), outputPath: String) : Unit = {
    val readsNumber = targetCountResult._2._2
    val sample = targetCountResult._1
    val fileObject = new File(outputPath + "/" + sample + ".rpkm") //TODO kkobylin file
    val pw = new PrintWriter(fileObject)
    var iterator = 1

    targetCountResult._2._1.collect().foreach(row => {
      val targetStart = row.getString(1).toInt
      val targetEnd = row.getString(2).toInt
      val cov = row.getLong(6)
      val exonLength = targetEnd - targetStart
      val rpkm = BigDecimal(calculateRpkm(cov, exonLength, readsNumber)).setScale(6, BigDecimal.RoundingMode.FLOOR).toDouble
      pw.write(iterator + "\t" + cov + "\t" + rpkm + "\n")
      iterator += 1
    })

    pw.close()
  }

  private def calculateRpkm(readCount: Long, exonLength: Int, totalReads: Long) : Double = {
    (scala.math.pow(10, 9) * readCount / exonLength) / totalReads
  }
}
