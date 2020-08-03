//package org.biodatageeks.sequila.tests.pileup
//
//import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
//import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
//import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, ScalaReflection}
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.unsafe.types.UTF8String
//import org.scalatest.FunSuite
//import org.biodatageeks.sequila.pileup.model.PileupRecord
//import org.biodatageeks.sequila.pileup.serializers.PileupProjection
//
//import scala.collection.mutable
//
//
////noinspection CorrespondsUnsorted
//class PileupProjectionTestSuite extends FunSuite
//  with DataFrameSuiteBase
//  with SharedSparkContext {
//
//  val contigMap: mutable.HashMap[String, String] = mutable.HashMap[String, String]("1" -> "1", "MT" -> "MT")
//  PileupProjection.setContigMap(contigMap)
//  val schema: StructType = ScalaReflection.schemaFor[PileupRecord].dataType.asInstanceOf[StructType]
//  val projection: UnsafeProjection = UnsafeProjection.create(schema)
//
//  test("Pileup projection with map") {
//    var contig = "1"
//    var position = 10
//    var base = "C"
//    var cov = 34.toShort
//    var refCount = 32.toShort
//    var altsCount = 2.toShort
//    val map = Map[Byte, Short]('A'.toByte -> 2.toShort, 'T'.toByte -> 3.toShort)
//
//    val rowPileupProjection = PileupProjection.convertToRow(contig, position, position, base, cov, refCount, altsCount, map, null)
//
//    val rowUnsafeProjection = projection.apply(InternalRow(
//      UTF8String.fromString(contig),
//      position, position,
//      UTF8String.fromString(base),
//      cov, refCount, altsCount, CatalystTypeConverters.convertToCatalyst(map)))
//
//    val dataPileupProjection = rowPileupProjection.getBytes
//    val dataUnsafeProjection = rowUnsafeProjection.getBytes
//
//    assert(dataPileupProjection.sameElements(dataUnsafeProjection))
//    assert(rowPileupProjection.getString(0) == contig)
//    assert(rowPileupProjection.getInt(1) == position)
//    assert(rowPileupProjection.getString(3) == base)
//    assert(rowPileupProjection.getInt(4) == cov)
//    assert(rowPileupProjection.getInt(5) == refCount)
//    assert(rowPileupProjection.getInt(6) == altsCount)
//    assert(rowPileupProjection.getMap(7).keyArray().toByteArray.sameElements(map.keySet.toArray[Byte]))
//    assert(rowPileupProjection.getMap(7).valueArray().toShortArray.sameElements(map.values.toArray[Short]))
//
//    contig = "MT"
//    position = 178545
//    base = "T"
//    cov = 34.toShort
//    refCount = 34.toShort
//    altsCount = 0.toShort
//
//    val dataPileupProjection2 = PileupProjection.convertToRow(contig, position, position, base, cov, refCount, altsCount, map, null)
//
//    assert(dataPileupProjection2.getString(0) == contig)
//    assert(dataPileupProjection2.getInt(1) == position)
//    assert(dataPileupProjection2.getString(3) == base)
//    assert(dataPileupProjection2.getInt(4) == cov)
//    assert(dataPileupProjection2.getInt(5) == refCount)
//    assert(dataPileupProjection2.getInt(6) == altsCount)
//    assert(dataPileupProjection2.getMap(7).keyArray().toByteArray.sameElements(map.keySet.toArray[Byte]))
//    assert(dataPileupProjection2.getMap(7).valueArray().toShortArray.sameElements(map.values.toArray[Short]))
//  }
//
//  test("Pileup projection with map = null") {
//    val contig = "1"
//    val position = 10
//    val base = "C"
//    val cov = 34.toShort
//    val refCount = 32.toShort
//    val altsCount = 2.toShort
//
//    val rowPileupProjection = PileupProjection.convertToRow(contig, position, position, base, cov, refCount, altsCount, null, null)
//
//    val rowUnsafeProjection = projection.apply(InternalRow(
//      UTF8String.fromString(contig),
//      position, position,
//      UTF8String.fromString(base),
//      cov, refCount, altsCount, null))
//
//
//    val dataPileupProjection = rowPileupProjection.getBytes
//    val dataUnsafeProjection = rowUnsafeProjection.getBytes
//
//    //noinspection CorrespondsUnsorted
//    assert(dataPileupProjection.sameElements(dataUnsafeProjection))
//    assert(rowPileupProjection.getString(0) == contig)
//    assert(rowPileupProjection.getInt(1) == position)
//    assert(rowPileupProjection.getString(3) == base)
//    assert(rowPileupProjection.getInt(4) == cov)
//    assert(rowPileupProjection.getInt(5) == refCount)
//    assert(rowPileupProjection.getInt(6) == altsCount)
//    assert(rowPileupProjection.isNullAt(7))
//
//  }
//
//  test("Pileup projection with qualmap") {
//    var contig = "1"
//    var position = 10
//    var base = "C"
//    var cov = 34.toShort
//    var refCount = 32.toShort
//    var altsCount = 2.toShort
//    val map = Map[Byte, Short]('A'.toByte -> 2.toShort, 'T'.toByte -> 3.toShort)
//    val qmap = Map[Byte, Array[Short]]('A'.toByte -> Array(1.toShort, 2.toShort))
//
//
//    val rowPileupProjection = PileupProjection.convertToRow(contig, position, position, base, cov, refCount, altsCount, map, qmap)
//
////    val rowUnsafeProjection = projection.apply(InternalRow(
////      UTF8String.fromString(contig),
////      position, position,
////      UTF8String.fromString(base),
////      cov, refCount, altsCount, CatalystTypeConverters.convertToCatalyst(map),
////      CatalystTypeConverters.convertToCatalyst(qmap)))
//
//    val dataPileupProjection = rowPileupProjection.getBytes
////    val dataUnsafeProjection = rowUnsafeProjection.getBytes
//
////    assert(dataPileupProjection.sameElements(dataUnsafeProjection))
//    assert(rowPileupProjection.getString(0) == contig)
//    assert(rowPileupProjection.getInt(1) == position)
//    assert(rowPileupProjection.getString(3) == base)
//    assert(rowPileupProjection.getInt(4) == cov)
//    assert(rowPileupProjection.getInt(5) == refCount)
//    assert(rowPileupProjection.getInt(6) == altsCount)
//
////    assert(rowPileupProjection.getMap(7).keyArray().toByteArray.sameElements(map.keySet.toArray[Byte]))
////    assert(rowPileupProjection.getMap(7).valueArray().toShortArray.sameElements(map.values.toArray[Short]))
//
////    println(rowPileupProjection.getMap(7).keyArray().toShortArray.mkString(" "))
////    println(rowPileupProjection.getMap(8))
//
////
////    contig = "MT"
////    position = 178545
////    base = "T"
////    cov = 34.toShort
////    refCount = 34.toShort
////    altsCount = 0.toShort
////
////    val dataPileupProjection2 = PileupProjection.convertToRow(contig, position, position, base, cov, refCount, altsCount, map, null)
////
////    assert(dataPileupProjection2.getString(0) == contig)
////    assert(dataPileupProjection2.getInt(1) == position)
////    assert(dataPileupProjection2.getString(3) == base)
////    assert(dataPileupProjection2.getInt(4) == cov)
////    assert(dataPileupProjection2.getInt(5) == refCount)
////    assert(dataPileupProjection2.getInt(6) == altsCount)
////    assert(dataPileupProjection2.getMap(7).keyArray().toByteArray.sameElements(map.keySet.toArray[Byte]))
////    assert(dataPileupProjection2.getMap(7).valueArray().toShortArray.sameElements(map.values.toArray[Short]))
//  }
//
//
//}
