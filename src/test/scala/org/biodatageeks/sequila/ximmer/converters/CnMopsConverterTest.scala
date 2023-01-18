//package org.biodatageeks.sequila.ximmer.converters
//
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}
//
//import java.io.File
//import scala.reflect.io.Directory
//
///**
//  * Testujemy tutaj samo formatowanie - oczekiwane pokrycie, różni się od tego które produkuje ximmer
//  */
//class CnMopsConverterTest extends FunSuite
//  with BeforeAndAfterAll {
//
//  val tempDir = "src/test/resources/ximmer/temp"
//  val ximmerResourceDir = "src/test/resources/ximmer"
//
//  override def beforeAll(): Unit = {
//    val directory = new Directory(new File(tempDir))
//    directory.createDirectory()
//  }
//
//  test("Convert to cnmops format") {
//    //given
//    val targetCountFiles = List(ximmerResourceDir + "/target_counts_XI001.csv",
//      ximmerResourceDir + "/target_counts_XI002.csv")
//    val sampleNames = List("XI001", "XI002")
//    val expectedJson = scala.io.Source.fromFile(ximmerResourceDir + "/cnmops/expected.cnmops.json")
//    //when
//    new CnMopsConverter().convertToCnMopsFormat(targetCountFiles, sampleNames, tempDir)
//    //then
//    val result = scala.io.Source.fromFile(tempDir + "/analysis.cnmops.cnvs.json")
//
//    JSONAssert.assertEquals(expectedJson.mkString, result.mkString, JSONCompareMode.NON_EXTENSIBLE)
//
//    expectedJson.close()
//    result.close()
//  }
//
//  override def afterAll: Unit = {
//    val directory = new Directory(new File(tempDir))
//    directory.deleteRecursively()
//  }
//}
