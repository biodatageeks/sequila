package org.biodatageeks.sequila.apps

import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import scala.reflect.io.Directory
import org.scalactic.{Equality, TolerantNumerics}

class TargetsCountTest extends FunSuite
  with BeforeAndAfter {

  val tempDir = "src/test/resources/ximmer/temp"
  val ximmerResourceDir = "src/test/resources/ximmer"

  test("Test calculated coverage for Codex, cnmops, exomedepth, conifer") {
    System.setProperty("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")

//    val arguments = List(
//      "-b", ximmerResourceDir + "/coverage_test_input",
//      "-t", ximmerResourceDir + "/coverage_test_input/hg38.bed",
//      "-o", tempDir)
//        TargetsCount.main(arguments.toArray)

    val epsilon = 3 //TODO kkobylin uzaleznic epsilon od ilosc rekordow w bamie i bedzie
    implicit val intEq: Equality[Int] = TolerantNumerics.tolerantIntEquality(epsilon)

    val result = scala.io.Source.fromFile(findCsvFile(tempDir + "/coverage_counts/target_counts_output/XI001"))
    val expectedCnmops = scala.io.Source.fromFile(ximmerResourceDir + "/cnmops/cnmops_expected_cov.txt").bufferedReader()
    val expectedCodex = scala.io.Source.fromFile(ximmerResourceDir + "/codex/codex_expected_cov.txt").bufferedReader()
    val expectedConifer = scala.io.Source.fromFile(ximmerResourceDir + "/conifer/conifer_expected_cov.txt").bufferedReader()
    val expectedExomeDepth = scala.io.Source.fromFile(ximmerResourceDir + "/exomedepth/exomedepth_expected_cov.txt").bufferedReader()

    var iterator = 1
    var errorsCnmops = 0
    var errorsCodex = 0
    var errorsConifer = 0
    var errorsExomedepth = 0
    for (line <- result.getLines()) {
      //println(iterator)
      val elements = line.split(",")
      val resultCnmopsCov = elements(4).toInt
      val resultCodexCov = elements(8).toInt
      val resultConiferCov = elements(6).toInt
      val resultExomedepthCov = elements(5).toInt

      val expectedCnmopsCov = expectedCnmops.readLine().toInt
      val expectedCodexCov = expectedCodex.readLine().toInt
      val expectedConiferCov = expectedConifer.readLine().toInt
      val expectedExomeDepthCov = expectedExomeDepth.readLine().toInt

      //assert(resultCnmopsCov === expectedCnmopsCov)
      //assert(resultCodexCov === expectedCodexCov)
      //assert(resultConiferCov === expectedConiferCov)
      //assert(resultExomedepthCov === expectedExomeDepthCov)
      if (!(resultCodexCov == expectedCodexCov)) {
        println("Expected: " + expectedCodexCov + " calculated: " + resultCodexCov + " iterator: " + iterator)
        errorsCodex += 1
      }
      if (!(resultCnmopsCov == expectedCnmopsCov)) {
        //println("Expected: " + expectedCnmopsCov + " calculated: " + resultCnmopsCov + " iterator: " + iterator)
        errorsCnmops += 1
      }
      if (!(resultConiferCov === expectedConiferCov)) {
//        println("Expected: " + expectedConiferCov + " calculated: " + resultConiferCov + " iterator: " + iterator)
        errorsConifer += 1
      }
      if (!(resultExomedepthCov == expectedExomeDepthCov)) {
//        println("Expected: " + expectedExomeDepthCov + " calculated: " + resultExomedepthCov + " iterator: " + iterator)
        errorsExomedepth += 1
      }
      iterator += 1
    }
    println(errorsCodex)
    println(errorsCnmops)
    println(errorsConifer)
    println(errorsExomedepth)

    result.close();expectedCnmops.close();expectedCodex.close();expectedConifer.close();expectedExomeDepth.close()
  }

//  override def afterAll: Unit = {
//    val directory = new Directory(new File(tempDir))
//    directory.deleteRecursively()
//  }

  private def findCsvFile(dirPath: String) : String = {
    val dir = new File(dirPath)
    if (!dir.exists() || !dir.isDirectory) {
      throw new IllegalArgumentException("Directory path should be provided")
    }

    dir.listFiles
      .filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.getPath)
      .map(x => x.replace("\\", "/"))
      .toList
      .head
  }
}
