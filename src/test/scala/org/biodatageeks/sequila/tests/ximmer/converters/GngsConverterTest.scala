package org.biodatageeks.sequila.tests.ximmer.converters

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.types._
import org.biodatageeks.sequila.ximmer.converters.GngsConverter
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
  val perBaseSchema: StructType = StructType(
    Seq(StructField("chr", StringType),
      StructField("start", StringType),
      StructField("end", StringType),
      StructField("ref", StringType),
      StructField("cov", ShortType)))

  before {
    val directory = new Directory(new File(tempDir))
    directory.createDirectory()
  }

  test("Convert to xhmm format") {
    //given
    val meanCoverageDF = spark.read
      .option("header", "false")
      .schema(meanCoverageSchema)
      .csv(ximmerResourceDir + "/converter_tests_input/mean_coverage_XI001.csv")
    val perBaseCoverageDF = spark.read
      .option("header", "false")
      .schema(perBaseSchema)
      .csv(ximmerResourceDir + "/converter_tests_input/per_base_coverage_XI001.csv")
    val expectedStats = scala.io.Source.fromFile(ximmerResourceDir + "/xhmm/expected.stats.tsv")
    val expectedSummary = scala.io.Source.fromFile(ximmerResourceDir + "/xhmm/expected.sample_interval_summary")
    //when
    new GngsConverter().calculateStatsAndConvertToGngsFormat(tempDir, "XI001", meanCoverageDF, perBaseCoverageDF)
    //then
    val resultStats = scala.io.Source.fromFile(tempDir + "/XI001.stats.tsv")
    val resultSummary = scala.io.Source.fromFile(tempDir + "/XI001.calc_target_covs.sample_interval_summary")

    assert(resultStats.getLines().mkString == expectedStats.getLines().mkString)
    assert(resultSummary.getLines().mkString == expectedSummary.getLines().mkString)

    expectedStats.close(); expectedSummary.close
    resultStats.close(); resultSummary.close
  }

  def after: Unit = {
    val directory = new Directory(new File(tempDir))
    directory.deleteRecursively()
  }

}
