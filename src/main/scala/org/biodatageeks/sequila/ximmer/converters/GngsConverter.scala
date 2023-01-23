package org.biodatageeks.sequila.ximmer.converters

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.utils.InternalParams

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer

class GngsConverter extends Serializable{

  def calculateStatsAndConvertToGngsFormat(outputPath: String, sample: String, meanCoverage : DataFrame,
                                           perBaseCoverage : DataFrame, median: Short): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    calculateAndWriteStats(perBaseCoverage, median, outputPath, sample, spark)
    writeSampleIntervalSummary(meanCoverage, outputPath, sample, spark)
  }

  private def calculateAndWriteStats(coveragesDf: DataFrame, median: Short, outputPath: String, sample: String, spark: SparkSession) : Unit = {
    val coveragesCount = coveragesDf.count()
    val coveragesSum = spark.sparkContext.longAccumulator("coveragesSum")
    val nrAbove1 = spark.sparkContext.longAccumulator("nrAbove1")
    val nrAbove5 = spark.sparkContext.longAccumulator("nrAbove5")
    val nrAbove10 = spark.sparkContext.longAccumulator("nrAbove10")
    val nrAbove20 = spark.sparkContext.longAccumulator("nrAbove20")
    val nrAbove50 = spark.sparkContext.longAccumulator("nrAbove50")

    coveragesDf.foreach(row => {
          val value = row.getShort(4).toInt
          val valueLong = value.toLong
          coveragesSum.add(valueLong)
          if (value > 1) nrAbove1.add(1L)
          if (value > 5) nrAbove5.add(1L)
          if (value > 10) nrAbove10.add(1L)
          if (value > 20) nrAbove20.add(1L)
          if (value > 50) nrAbove50.add(1L)
        }
      )

    val mean = coveragesSum.value / coveragesCount.toDouble
    val perc_bases_above_1 = nrAbove1.value.toDouble / coveragesCount * 100
    val perc_bases_above_5 = nrAbove5.value.toDouble / coveragesCount * 100
    val perc_bases_above_10 = nrAbove10.value.toDouble / coveragesCount * 100
    val perc_bases_above_20 = nrAbove20.value.toDouble / coveragesCount * 100
    val perc_bases_above_50 = nrAbove50.value.toDouble / coveragesCount * 100

    val filename = outputPath + "/" +sample + ".stats.tsv"
    val fileObject = new File(filename)
    val pw = new PrintWriter(fileObject)
    val resultList = ListBuffer[String]()
    val statsHeader = "Median Coverage\tMean Coverage\tperc_bases_above_1\tperc_bases_above_5\tperc_bases_above_10\t" +
      "perc_bases_above_20\tperc_bases_above_50"
    pw.write(statsHeader)
    resultList += statsHeader
    pw.write("\n")
    val statsLine = median + "\t" + mean + "\t" + perc_bases_above_1 + "\t" + perc_bases_above_5 + "\t" + perc_bases_above_10 + "\t" + perc_bases_above_20 + "\t" + perc_bases_above_50 + "\t"
    pw.write(statsLine)
    resultList += statsLine
    println(s"Write file " + outputPath + "/sample_interval_summary/" + sample + ".stats.tsv")
    pw.close()

    if (spark.conf.get(InternalParams.saveAsSparkFormat).toBoolean) {
      import spark.implicits._
      val resultDF = spark.sparkContext.parallelize(resultList).toDF()
      resultDF.write
        .option("delimiter", "\t")
        .csv(outputPath + "/spark" + "/" + sample + "-stats")
    }
  }

  private def writeSampleIntervalSummary(meanCoverage: DataFrame, outputPath: String, sample: String, spark: SparkSession): Unit = {
    import spark.implicits._

    val regionsAndMeans = meanCoverage.mapPartitions(rowIterator => rowIterator.map(
      row => {
        val chr = row.getString(0)
        val start = row.getString(1)
        var end = row.getString(2).toInt
        end = end - 1
        val mean = row.getDouble(3).toString
        val region = chr + ":" + start + "-" + end.toString
        (region, mean)
      }
    )).collect()

    var regions = regionsAndMeans
      .map(x => x._1)
      .toList
    regions = "sample" +: regions

    var means = regionsAndMeans
      .map(x => x._2)
      .toList
    means = sample +: means

    val filename = outputPath + "/" + sample + ".calc_target_covs.sample_interval_summary"
    val fileObject = new File(filename)
    val pw = new PrintWriter(fileObject)
    val resultList = ListBuffer[String]()
    pw.write(regions.mkString("\t"))
    resultList += regions.mkString("\t")
    pw.write("\n")
    pw.write(means.mkString("\t"))
    resultList += means.mkString("\t")
    println(s"Write file " + outputPath + "/sample_interval_summary/" + sample + ".calc_target_covs.sample_interval_summary")
    pw.close()

    if (spark.conf.get(InternalParams.saveAsSparkFormat).toBoolean) {
      import spark.implicits._
      val resultDF = spark.sparkContext.parallelize(resultList).toDF()
      resultDF.write
        .option("delimiter", "\t")
        .csv(outputPath + "/spark" + "/" + sample + "-summary")
    }
  }

}
