package org.biodatageeks.sequila.tests.pileup
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.types.StructType
import org.biodatageeks.sequila.tests.base.BAMBaseTestSuite

import scala.collection.mutable
import org.apache.spark.unsafe.types.UTF8String
import org.biodatageeks.sequila.pileup.serializers.PileupProjection

case class TestRecord (array: Map[Int, Int])
case class TestShortRecord(array:Map[Short,Short])
case class ByteMapRecord(array: Map[Byte, Int])
case class StringMapRecord(array: Map[String, Int])
case class ComplexRecord(contig: String, position:Int, array: Map[Byte, Int])
case class ContigRecord(contig: String,  array: Map[Byte, Int])
case class TwoStringRecord(contig: String, pos: Int, contig2:String, array: Map[Byte, Int])
case class SimpleByteShortRecord(array:Map[Byte, Short])

class MapProjectionTestSuite extends BAMBaseTestSuite with SharedSparkContext{

  val contigMap: mutable.HashMap[String, String] = mutable.HashMap[String,String] ("1" -> "1", "MT" -> "MT")

  test ("save short map") {
    val map = Map [Byte,Short] ('A'.toByte-> 20.toShort, '3'.toByte->30.toShort, '4'.toByte->40.toShort, '5'.toByte->50.toShort, '6'.toByte->60.toShort )
    val schema = ScalaReflection.schemaFor[SimpleByteShortRecord].dataType.asInstanceOf[StructType]
    val projection = UnsafeProjection.create(schema)
    val row = projection.apply(InternalRow(CatalystTypeConverters.convertToCatalyst(map)) )
    val projectionMap = row.getMap(0)

    val keyArraySize = projectionMap.keyArray().getSizeInBytes
    val valueArraySize = projectionMap.valueArray().getSizeInBytes
    val mapTotalSize = projectionMap.getSizeInBytes

    val array = row.getBytes
//    println (s"--- ${array.length} --- ")
//    for (i <- 0 to array.length - 1)
//      if (array(i) != 0)
//        println (s"$i -> ${array(i)}")

    PileupProjection.setContigMap(contigMap)
    val (mapSize, keySize, valSize) = PileupProjection.calculateAltsMapSizes(map)


    assert(mapTotalSize == mapSize)
    assert(keySize == keyArraySize)
    assert(valSize == valueArraySize)

    val data = new Array[Byte](mapSize + 2*8)
    PileupProjection.writeMap(data,map,8, 16)
    assert(data.sameElements(array))

    val row2 = new UnsafeRow(1)
    row2.pointTo(data, data.length)

    val map2 = row2.getMap(0)

    assert(map2.keyArray().toByteArray.sameElements(map.keySet.toArray[Byte]))
    assert(map2.valueArray().toShortArray.sameElements(map.values.toArray[Short]))
  }


  test ("save byte map") {
    val map = Map [Byte,Int] ('A'.toByte-> 20, 'C'.toByte->30, 'G'.toByte->40, 'T'.toByte->50)
    val schema = ScalaReflection.schemaFor[ByteMapRecord].dataType.asInstanceOf[StructType]

    val projection = UnsafeProjection.create(schema)

    val row = projection.apply(InternalRow(CatalystTypeConverters.convertToCatalyst(map)) )
    val mapt = row.getMap(0)

    val array = row.getBytes
  }


  test ("save complex") {
    val map = Map [Byte,Int] ('A'.toByte -> 20, 'C'.toByte->30, 'G'.toByte->40, 'T'.toByte->50)
    val schema = ScalaReflection.schemaFor[ComplexRecord].dataType.asInstanceOf[StructType]

    val projection = UnsafeProjection.create(schema)

    val row = projection.apply(InternalRow(UTF8String.fromString("MT"), 110,CatalystTypeConverters.convertToCatalyst(map)) )
    val mapt = row.getMap(2)
    val array = row.getBytes

  }


  test ("save contig + array") {
    val map = Map [Byte,Int] ('A'.toByte -> 20, 'C'.toByte->30, 'G'.toByte->40, 'T'.toByte->50)
    val schema = ScalaReflection.schemaFor[ContigRecord].dataType.asInstanceOf[StructType]

    val projection = UnsafeProjection.create(schema)

    val row = projection.apply(InternalRow(UTF8String.fromString("MT"),CatalystTypeConverters.convertToCatalyst(map)) )
    val mapt = row.getMap(1)
    val array = row.getBytes


  }


  test ("save two contigs + int + map") {
    val map = Map [Byte,Int] ('A'.toByte -> 20, 'C'.toByte->30, 'G'.toByte->40, 'T'.toByte->50)
    val schema = ScalaReflection.schemaFor[TwoStringRecord].dataType.asInstanceOf[StructType]

    val projection = UnsafeProjection.create(schema)

    val row = projection.apply(InternalRow(UTF8String.fromString("MT"),110,UTF8String.fromString("MT"), CatalystTypeConverters.convertToCatalyst(map)) )
    val mapt = row.getMap(4)
    val array = row.getBytes
//    println (s"--- Complex record data array ${array.length} --- ")
//    for (i <- 0 to array.length - 1)
//      if (array(i) != 0)
//        println (s"$i -> ${array(i)}")
  }

}
