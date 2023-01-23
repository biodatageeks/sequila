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
    val targetsPath = ximmerResourceDir + "/coverage_test_input/mini.bed"
    val bamFile = ximmerResourceDir + "/coverage_test_input/XI001.hg38.sorted.bam"
    val expectedMeanCoverage = scala.io.Source.fromFile(ximmerResourceDir + "/xhmm/expected_mean_coverage.csv").bufferedReader()
    val expectedPerBaseCoverage = scala.io.Source.fromFile(ximmerResourceDir + "/xhmm/expected_per_base_coverage.csv").bufferedReader()
    //when
    val result = new PerBaseCoverage().calculatePerBaseCoverage(ss, List(bamFile), targetsPath)
    //then
    assertEquals("XI001", result.head._1)
    assertEquals(6, result.head._2._3)
    val meanCoverageDF = result.head._2._1
    val perBaseCoverageDF = result.head._2._2


    meanCoverageDF.collect().foreach(row => {
      val lineElements = expectedMeanCoverage.readLine().split(",")
      val expectedChr = lineElements(0)
      val expectedStart = lineElements(1).toInt
      val expectedEnd = lineElements(2).toInt
      val expectedCov = lineElements(3).toDouble

      assertEquals(expectedChr, row.getString(0))
      assertEquals(expectedStart, row.getString(1).toInt)
      assertEquals(expectedEnd, row.getString(2).toInt)
      assertTrue(row.getDouble(3) - expectedCov <= 5)
    })

    //Differences here are due to fact that gngs shorten reads - replace alignmentEnd while reading bam with mateStart
    perBaseCoverageDF.collect().foreach(row => {
      val lineElements = expectedPerBaseCoverage.readLine().split(",")
      val expectedChr = lineElements(0)
      val expectedStart = lineElements(1).toInt
      val expectedCov = lineElements(4).toShort

      assertEquals(expectedChr, row.getString(0))
      assertEquals(expectedStart, row.getInt(1))
      assertEquals(row.getInt(1), row.getInt(2))
      assertTrue(row.getShort(4) - expectedCov <= 5)
    })

    expectedMeanCoverage.close();expectedPerBaseCoverage.close()
  }
}
