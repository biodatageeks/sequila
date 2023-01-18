//package org.biodatageeks.sequila.ximmer.converters
//
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//
//import java.io.File
//import scala.reflect.io.Directory
//
///**
//  * Testujemy tylko formatowanie. Oczekiwane pokrycie zostało lekko zmodyfikowane
//  * Ponieważ oryginalny conifer błędnie liczy ilość odczytów w próbce, wyliczona wartość
//  * została zmodyfikowana do poprawnej ilości odczytów w próbce
//  */
//class ConiferConverterTest extends FunSuite
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
//  test("Convert to conifer format") {
//    //given
//    val targetCountFile = ximmerResourceDir + "/target_counts_XI001.csv"
//    val bamInfoFile = ximmerResourceDir + "/conifer/sample.bam_info"
//    val expected = scala.io.Source.fromFile(ximmerResourceDir + "/conifer/expected.XI001.rpkm")
//    //when
//    new ConiferConverter().convertToConiferFormat("XI001", targetCountFile, bamInfoFile, tempDir)
//    //then
//    val result = scala.io.Source.fromFile(tempDir + "/XI001.rpkm")
//
//    assert(expected.getLines().mkString == result.getLines().mkString)
//
//    expected.close()
//    result.close()
//  }
//
//  override def afterAll: Unit = {
//    val directory = new Directory(new File(tempDir))
//    directory.deleteRecursively()
//  }
//}