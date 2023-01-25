package org.biodatageeks.sequila.tests.ximmer

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.apps.PileupApp.createSparkSessionWithExtraStrategy
import org.biodatageeks.sequila.ximmer.TargetCounts
import org.junit.Assert.assertEquals
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.{BeforeAndAfter, FunSuite}

class TargetsCountTest extends FunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfter {

  val ximmerResourceDir: String = getClass.getResource("/ximmer").getPath

  test("Test calculated coverage for Codex, cnmops, exomedepth, conifer") {
    //given
    val spark = createSparkSessionWithExtraStrategy()
    val ss = SequilaSession(spark)
    val targetsPath = ximmerResourceDir + "/coverage_test_input/hg38.bed"
    val bamFile = ximmerResourceDir + "/coverage_test_input/XI001.hg38.sorted.bam"
    val expectedCnmops = scala.io.Source.fromFile(ximmerResourceDir + "/cnmops/expected_cov_cnmops_XI001.txt").bufferedReader()
    val expectedCodex = scala.io.Source.fromFile(ximmerResourceDir + "/codex/expected_cov_codex_XI001.txt").bufferedReader()
    val expectedConifer = scala.io.Source.fromFile(ximmerResourceDir + "/conifer/expected_cov_conifer_XI001.txt").bufferedReader()
    val expectedExomeDepth = scala.io.Source.fromFile(ximmerResourceDir + "/exomedepth/expected_cov_ed_XI001.txt").bufferedReader()
    val epsilon = 1
    implicit val longEq: Equality[Long] = TolerantNumerics.tolerantLongEquality(epsilon)
    //when
    val result = new TargetCounts().calculateTargetCounts(ss, targetsPath, List(bamFile), saveBamInfo = true)
    //then
    var errorsCodex = 0
    var errorsConifer = 0

    assertEquals("XI001", result.head._1)
    assertEquals(626359, result.head._2._2.first().getLong(0))
    val resultDF = result.head._2._1
    resultDF.collect().foreach(row => {
      val expectedCnmopsCov = expectedCnmops.readLine().toLong
      val expectedCodexCov = expectedCodex.readLine().toLong
      val expectedConiferCov = expectedConifer.readLine().toLong
      val expectedExomeDepthCov = expectedExomeDepth.readLine().toLong

      val resultCodexCov = row.getLong(3)
      val resultCnmopsCov = row.getLong(4)
      val resultExomedepthCov = row.getLong(5)
      val resultConiferCov = row.getLong(6)

      assertEquals(resultCnmopsCov, expectedCnmopsCov)
      assertEquals(resultExomedepthCov, expectedExomeDepthCov)
      // Differences here comes from calculate read length method. When Codex calculate length from "CIGAR" fields, does not
      // count deletes, sequilla does. Reads in Codex may be shorter and that's why in 2 targets coverage is smaller
      if (!(resultCodexCov == expectedCodexCov)) {
        assertTrue(resultCodexCov - expectedCodexCov == 1)
        errorsCodex += 1
      }
      //Differences here are because conifer read contig from "referenceName" fields instead of "contig".
      //When read is unmapped, contig is null and is filter out in sequila, but referenceName may be filled and counted in Conifer
      if (!(resultConiferCov === expectedConiferCov)) {
        assertTrue((resultConiferCov / expectedConiferCov.toDouble) >= 0.984)
        errorsConifer += 1
      }
    })
    assertEquals(2, errorsCodex)
    assertEquals(3, errorsConifer)

    expectedCnmops.close();expectedCodex.close();expectedConifer.close();expectedExomeDepth.close()
  }
}
