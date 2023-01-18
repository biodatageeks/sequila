//package org.biodatageeks.sequila.ximmer.converters
//
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}
//
//import java.io.File
//import scala.reflect.io.Directory
//
//
//
//class CodexConverterTest extends FunSuite
//  with BeforeAndAfterAll{
//
//  val tempDir = "src/test/resources/ximmer/temp"
//  val ximmerResourceDir = "src/test/resources/ximmer"
//
//  override def beforeAll(): Unit = {
//    val directory = new Directory(new File(tempDir))
//    directory.createDirectory()
//  }
//
//  test("Convert to codex format") {
//    //given
//    val targetCountFiles = List(ximmerResourceDir + "/target_counts_XI001.csv",
//      ximmerResourceDir + "/target_counts_XI002.csv")
//    val expectedChr1 = scala.io.Source.fromFile(ximmerResourceDir + "/codex/expected.codex_coverage.chr1.json")
//    val expectedChr2 = scala.io.Source.fromFile(ximmerResourceDir + "/codex/expected.codex_coverage.chr2.json")
//    val expectedChrX = scala.io.Source.fromFile(ximmerResourceDir + "/codex/expected.codex_coverage.chrX.json")
//    //when
//    new CodexConverter().convertToCodexFormat(targetCountFiles, tempDir)
//    //then
//    val resultChr1 = scala.io.Source.fromFile(tempDir + "/analysis.codex_coverage.chr1.json")
//    val resultChr2 = scala.io.Source.fromFile(tempDir + "/analysis.codex_coverage.chr2.json")
//    val resultChrX = scala.io.Source.fromFile(tempDir + "/analysis.codex_coverage.chrX.json")
//
//    JSONAssert.assertEquals(expectedChr1.mkString, resultChr1.mkString, JSONCompareMode.NON_EXTENSIBLE)
//    JSONAssert.assertEquals(expectedChr2.mkString, resultChr2.mkString, JSONCompareMode.NON_EXTENSIBLE)
//    JSONAssert.assertEquals(expectedChrX.mkString, resultChrX.mkString, JSONCompareMode.NON_EXTENSIBLE)
//
//    expectedChr1.close(); expectedChr2.close(); expectedChrX.close()
//    resultChr1.close(); resultChr2.close(); resultChrX.close()
//  }
//
//  override def afterAll: Unit = {
//    val directory = new Directory(new File(tempDir))
//    directory.deleteRecursively()
//  }
//
//}
