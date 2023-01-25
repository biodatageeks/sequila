package org.biodatageeks.sequila.ximmer.converters

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.utils.InternalParams

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer

class ConiferConverter {

  def convertToConiferFormat(targetCountResult: (String, (DataFrame, DataFrame)), outputPath: String) : Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val readsNumber = targetCountResult._2._2.first().getLong(0)
    val sample = targetCountResult._1
    val filename = outputPath + "/" + sample + ".rpkm"
    val fileObject = new File(filename)
    val pw = new PrintWriter(fileObject)
    var iterator = 1

    val resultList = ListBuffer[String]()
    targetCountResult._2._1.collect().foreach(row => {
      val targetStart = row.getString(1).toInt
      val targetEnd = row.getString(2).toInt
      val cov = row.getLong(6)
      val exonLength = targetEnd - targetStart
      val rpkm = BigDecimal(calculateRpkm(cov, exonLength, readsNumber)).setScale(6, BigDecimal.RoundingMode.FLOOR).toDouble
      val line = iterator + "\t" + cov + "\t" + rpkm
      pw.write(line + "\n")
      resultList += line
      iterator += 1
    })

    pw.close()

    if (spark.conf.get(InternalParams.saveAsSparkFormat).toBoolean) {
      import spark.implicits._
      val resultDF = spark.sparkContext.parallelize(resultList).toDF()
      resultDF.write
        .option("delimiter", "\t")
        .csv(outputPath + "/spark" + "/" + sample)
    }
  }

  private def calculateRpkm(readCount: Long, exonLength: Int, totalReads: Long) : Double = {
    (scala.math.pow(10, 9) * readCount / exonLength) / totalReads
  }
}
