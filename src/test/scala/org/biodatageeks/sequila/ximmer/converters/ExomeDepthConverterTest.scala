package org.biodatageeks.sequila.ximmer.converters

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.File
import scala.reflect.io.Directory

/**
  * Testujemy tylko konwerter, z oczekiwanego rezultatu została usunięta kolumna GC.
  * Oczekiwane pokrycie zostało również zmodyfikowane
  */
class ExomeDepthConverterTest extends FunSuite
  with BeforeAndAfterAll {

  val tempDir = "src/test/resources/ximmer/temp"
  val ximmerResourceDir = "src/test/resources/ximmer"

  override def beforeAll(): Unit = {
    val directory = new Directory(new File(tempDir))
    directory.createDirectory()
  }

  test("Convert to exomeDepth format") {
    //given
    val targetCountFiles = List(ximmerResourceDir + "/target_counts_XI001.csv",
      ximmerResourceDir + "/target_counts_XI002.csv")
    val sampleNames = List("XI001", "XI002")
    val expectedChr1Json = scala.io.Source.fromFile(ximmerResourceDir + "/exomedepth/expected.chr1.counts.tsv")
    val expectedChr2Json = scala.io.Source.fromFile(ximmerResourceDir + "/exomedepth/expected.chr2.counts.tsv")
    val expectedChrXJson = scala.io.Source.fromFile(ximmerResourceDir + "/exomedepth/expected.chrX.counts.tsv")
    //when
    new ExomeDepthConverter().convertToExomeDepthFormat(targetCountFiles, sampleNames, tempDir)
    //then
    val resultChr1 = scala.io.Source.fromFile(tempDir + "/analysis.chr1.counts.tsv")
    val resultChr2 = scala.io.Source.fromFile(tempDir + "/analysis.chr2.counts.tsv")
    val resultChrX = scala.io.Source.fromFile(tempDir + "/analysis.chrX.counts.tsv")

    assert(expectedChr1Json.getLines().mkString == resultChr1.getLines().mkString)
    assert(expectedChr2Json.getLines().mkString == resultChr2.getLines().mkString)
    assert(expectedChrXJson.getLines().mkString == resultChrX.getLines().mkString)

    expectedChr1Json.close(); expectedChr2Json.close(); expectedChrXJson.close()
    resultChr1.close(); resultChr2.close(); resultChrX.close()
  }

  override def afterAll: Unit = {
    val directory = new Directory(new File(tempDir))
    directory.deleteRecursively()
  }
}

