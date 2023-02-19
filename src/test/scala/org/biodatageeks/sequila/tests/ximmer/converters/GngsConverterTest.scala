/**
  * Created by Krzysztof Kobyliński
  */
package org.biodatageeks.sequila.tests.ximmer.converters

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.types._
import org.biodatageeks.sequila.ximmer.converters.GngsConverter
import org.biodatageeks.sequila.utils.InternalParams
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import scala.reflect.io.Directory

class GngsConverterTest extends FunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfter {

  val tempDir = "src/test/resources/ximmer/temp"
  val ximmerResourceDir = "src/test/resources/ximmer"
  val meanCoverageSchema: StructType = StructType(
    Seq(StructField("chr", StringType),
      StructField("start", StringType),
      StructField("end", StringType),
      StructField("mean_cov", DoubleType)))
  val statsSchema: StructType = StructType(
    Seq(StructField("median", ShortType),
      StructField("mean", DoubleType),
      StructField("readsNr", LongType),
      StructField("above1", LongType),
      StructField("above5", LongType),
      StructField("above10", LongType),
      StructField("above20", LongType),
      StructField("above50", LongType)))

  before {
    val directory = new Directory(new File(tempDir))
    directory.createDirectory()
  }

  test("Convert to xhmm format") {
    //given
    spark.conf.set(InternalParams.saveAsSparkFormat, "false")
    val meanCoverageDF = spark.read
      .option("header", "false")
      .schema(meanCoverageSchema)
      .csv(ximmerResourceDir + "/converter_tests_input/mean_coverage_XI001.csv")
    val statsDF = spark.read
      .option("header", "false")
      .schema(statsSchema)
      .csv(ximmerResourceDir + "/converter_tests_input/stats_XI001.csv")
    val expectedStats = scala.io.Source.fromFile(ximmerResourceDir + "/xhmm/expected.stats.tsv")
    val expectedSummary = scala.io.Source.fromFile(ximmerResourceDir + "/xhmm/expected.sample_interval_summary")
    //when
    new GngsConverter().calculateStatsAndConvertToGngsFormat(tempDir, "XI001", meanCoverageDF, statsDF)
    //then
    val resultStats = scala.io.Source.fromFile(tempDir + "/XI001.stats.tsv")
    val resultSummary = scala.io.Source.fromFile(tempDir + "/XI001.calc_target_covs.sample_interval_summary")

    assert(resultStats.getLines().mkString.trim == expectedStats.getLines().mkString)
    assert(resultSummary.getLines().mkString.trim == expectedSummary.getLines().mkString)

    expectedStats.close(); expectedSummary.close
    resultStats.close(); resultSummary.close
  }

 after {
    val directory = new Directory(new File(tempDir))
    directory.deleteRecursively()
  }

}
