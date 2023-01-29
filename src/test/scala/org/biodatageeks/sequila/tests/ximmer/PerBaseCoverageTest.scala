package org.biodatageeks.sequila.tests.ximmer

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.apps.PileupApp.createSparkSessionWithExtraStrategy
import org.biodatageeks.sequila.ximmer.PerBaseCoverage
import org.junit.Assert.assertEquals
import org.scalatest.{BeforeAndAfter, FunSuite}

class PerBaseCoverageTest extends FunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfter  {

  val ximmerResourceDir: String = getClass.getResource("/ximmer").getPath

  test("Test calculated perBaseCoverage and meanCoverage") {
    val spark = createSparkSessionWithExtraStrategy()
    val ss = SequilaSession(spark)
    val targetsPath = ximmerResourceDir + "/coverage_test_input/hg38.bed"
    val bamFile = ximmerResourceDir + "/coverage_test_input/XI001.hg38.sorted.bam"
    val expectedMeanCoverage = scala.io.Source.fromFile(ximmerResourceDir + "/xhmm/expected_mean_coverage.csv").bufferedReader()
    val expectedStats = scala.io.Source.fromFile(ximmerResourceDir + "/xhmm/expected_calculated_stats.csv").bufferedReader()
    //when
    val result = new PerBaseCoverage().calculatePerBaseCoverage(ss, List(bamFile), targetsPath)
    //then
    assertEquals("XI001", result.head._1)
    val meanCoverageDF = result.head._2._1
    val statsDF = result.head._2._2

    //Differences here are due to fact that gngs shorten reads - replace alignmentEnd while reading bam with mateStart
    meanCoverageDF.collect().foreach(row => {
      val lineElements = expectedMeanCoverage.readLine().split(",")
      val expectedChr = lineElements(0)
      val expectedStart = lineElements(1).toInt
      val expectedEnd = lineElements(2).toInt
      val expectedCov = lineElements(3).toDouble

      assertEquals(expectedChr, row.getString(0))
      assertEquals(expectedStart, row.getString(1).toInt)
      assertEquals(expectedEnd, row.getString(2).toInt)

      val percDiff = (row.getDouble(3) / expectedCov - 1) * 100
      val diff = row.getDouble(3) / expectedCov
      val bothEqualsZero = row.getDouble(3) == 0 && expectedCov == 0
      assertTrue(bothEqualsZero || percDiff < 13 || diff <= 3)
    })

    val expectedStatsLine = expectedStats.readLine().split(",")
    val row = statsDF.first()
    val median = row.getShort(0)
    val mean = row.getDouble(1)
    val countNr = row.getLong(2).toDouble
    val perc_bases_above_1 = row.getLong(3) / countNr * 100
    val perc_bases_above_5 = row.getLong(4) / countNr * 100
    val perc_bases_above_10 = row.getLong(5) / countNr * 100
    val perc_bases_above_20 = row.getLong(6) / countNr * 100
    val perc_bases_above_50 = row.getLong(7) / countNr * 100
    assertTrue(median - expectedStatsLine(0).toInt <= 2)
    assertTrue(mean - expectedStatsLine(1).toDouble <= 2.2)
    assertTrue(perc_bases_above_1 - expectedStatsLine(2).toDouble <= 0.2)
    assertTrue(expectedStatsLine(3).toDouble - perc_bases_above_5 <= 0.2)
    assertTrue(perc_bases_above_10 - expectedStatsLine(4).toDouble <= 2)
    assertTrue(perc_bases_above_20 - expectedStatsLine(5).toDouble <= 2)
    assertTrue(perc_bases_above_50 - expectedStatsLine(6).toDouble <= 2.2)

    expectedMeanCoverage.close();expectedStats.close()
  }
}
