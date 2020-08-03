package org.biodatageeks.sequila.pileup.serializers

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

object PileupProjection {

  private val wordSize = 8
  private val intSize = 4
  private val shortSize = 2
  private val byteSize = 1

  private val baseByteMap = new mutable.HashMap[Char, Array[Byte]]()
  """
    |A
    |C
    |G
    |M
    |N
    |R
    |T
    |a
    |c
    |g
    |n
    |t
    |""".stripMargin
    .foreach( base =>   baseByteMap += (base -> UTF8String.fromString(base.toString).getBytes ) )

  var contigMap: mutable.HashMap[String,String]= new mutable.HashMap[String,String]()
  var contigByteMap: mutable.HashMap[String, Array[Byte]]= new mutable.HashMap[String, Array[Byte]] ()

  def setContigMap (cMap:mutable.HashMap[String,String] ) ={
    cMap
      .keys
      .foreach { contig =>
        if (!contigByteMap.contains(contig))
          contigByteMap += (contig -> UTF8String.fromString(contig).getBytes)
      }
  }

  private def getBytesForSequence(seq: String): Array[Byte] = {
    if (seq.length == 1)
      return baseByteMap(seq.head)
    val arr = new Array[Byte](seq.length)
    for (i <- 0 to seq.length - 1)
      arr(i) = baseByteMap(seq.substring(i, i + 1).head).head
    arr
  }


  private def writeNumber(data: Array[Byte], value: Int, offset: Int): Unit = {
    var i = 0
    while (i < intSize) {
      data(i + offset) = (value >> i * wordSize).toByte
      i += 1
    }
  }

  private def writeShort(data: Array[Byte], value: Short, offset: Int): Unit = {
    var i = 0
    while (i < shortSize) {
      data(i + offset) = (value >> i * wordSize).toByte
      i += 1
    }
  }

  private def writeString(data: Array[Byte], value: String, fieldOffset: Int, valueOffset: Int, valInBytes: Array[Byte] = null): Unit = {
    val len = value.length
    val valueInBytes = if (valInBytes != null) valInBytes else UTF8String.fromString(value).getBytes
    data(fieldOffset) = len.toByte
    data(fieldOffset + 4) = valueOffset.toByte
    var i = 0
    while (i < len) {
      data(valueOffset + i) = valueInBytes(i)
      i += 1
    }
  }

  private def roundUp(value: Int, divisibleBy:Int): Int = (Math.ceil(value.toDouble / divisibleBy) * divisibleBy).toInt

  private def calculateArrayNullRegionLen(numElements: Int): Int = {
    if (roundUp(Math.ceil(numElements / wordSize.toDouble).toInt, wordSize) != 0)
      roundUp(Math.ceil(numElements / wordSize.toDouble).toInt, wordSize)
    else
      wordSize
  }

  private def calculateArraySize(arr: Array[Short]): Int =
    wordSize + calculateArrayNullRegionLen(arr.length) + roundUp(arr.length *shortSize, wordSize)

  def calculateAltsMapSizes(map: Map[Byte,Short]): (Int, Int, Int)= {
    val numElements = map.size
    val arrayNullRegionLen = calculateArrayNullRegionLen(numElements)

    val keysArraySize = wordSize + arrayNullRegionLen + roundUp(numElements*byteSize, wordSize)
    val valuesArraySize = wordSize + arrayNullRegionLen + roundUp(numElements*shortSize, wordSize)
    val mapSize = wordSize + keysArraySize + valuesArraySize
    (mapSize, keysArraySize, valuesArraySize)
  }


  def calculateQualMapSizes(map: Map[Byte,Array[Short]]): (Int, Int, Int)= {
    val numElements = map.size
    val arrayNullRegionLen = calculateArrayNullRegionLen(numElements)

    val keysArraySize = wordSize + arrayNullRegionLen + roundUp(numElements*byteSize, wordSize)
    var shortArraysSize = 0
    for (k <- map.keys)
      shortArraysSize += calculateArraySize(map(k))

    val valuesArraySize = wordSize + arrayNullRegionLen + roundUp(shortArraysSize, wordSize) + map.size *wordSize
    val mapSize = wordSize + keysArraySize + valuesArraySize
    (mapSize, keysArraySize, valuesArraySize)
  }


  private def writeMapFixedHeader(data: Array[Byte], mapSize: Int, baseOffset: Int, arraysOffset: Int): Unit ={
    // write mapSize in first available index [0]
    writeNumber(data, mapSize, baseOffset)

    // write keysOffset -baseOffset in nex available index [+4]
    writeNumber(data, arraysOffset, baseOffset + intSize)

  }

  private def writeNestedArray(data: Array[Byte], array: Array[Array[Short]], offset: Int): Unit = {
    // in first 8 bytes write num elements
    writeNumber(data, array.length, offset)

    //null region next 8 bytes

    val nullArrayRegionLen = calculateArrayNullRegionLen(array.length)
    var elementsOffset = offset + wordSize + nullArrayRegionLen

    val fixedRegionLen = array.length*wordSize
    var offsetWRTBase = wordSize + nullArrayRegionLen + fixedRegionLen
    for (i <- array.indices) {
      data(elementsOffset + i*wordSize) = array(i).length.toByte
      writeNumber(data,offsetWRTBase, elementsOffset + i*wordSize + 4)
      //data(elementsOffset + i*wordSize + 4) = offsetWRTBase.toByte
      writeArray(data, array(i), offsetWRTBase + offset)
      val singleArraySize2 = wordSize + nullArrayRegionLen + roundUp(array(i).length * shortSize, wordSize)
      val singleArraySize = calculateArraySize(array(i))

      offsetWRTBase += singleArraySize
    }
  }


  private def writeArray(data: Array[Byte], array: Array[Byte], offset: Int): Unit = {
    // in first 8 bytes write num elements
    writeNumber(data, array.length, offset)

    //null region next 8 bytes

    val nullArrayRegionLen = calculateArrayNullRegionLen(array.length)
    val elementsOffset = offset + wordSize + nullArrayRegionLen
    for (i <- array.indices)
      data(elementsOffset + i) = array(i)
  }

  private def writeArray(data: Array[Byte], array: Array[Short], offset: Int): Unit = {
    // in first 8 bytes write num elements
    writeNumber(data, array.length, offset)

    //null region next 8 bytes

    val nullArrayRegionLen = calculateArrayNullRegionLen(array.length)
    val elementsOffset = offset + wordSize + nullArrayRegionLen

    for (i <- array.indices)
      writeShort(data, array(i), elementsOffset + i * shortSize)
  }

    def writeMap(data: Array[Byte], map: Map[Byte,Short], headerOffset: Int, arraysOffset:Int): Unit = {
    val (mapSize, keysArraySize, _) = calculateAltsMapSizes(map)

    val keysOffset = arraysOffset + wordSize
    val valuesOffset = keysOffset + keysArraySize

    writeMapFixedHeader(data, mapSize, headerOffset, arraysOffset)
    writeNumber(data, keysArraySize, arraysOffset)
    writeArray(data, map.keySet.toArray[Byte], keysOffset)
    writeArray(data, map.values.toArray[Short], valuesOffset)
  }

  def writeQualsMap(data: Array[Byte], map: Map[Byte,Array[Short]], headerOffset: Int, arraysOffset:Int): Unit = {
    val (mapSize, keysArraySize, _) = calculateQualMapSizes(map)

    val keysOffset = arraysOffset + wordSize
    val valuesOffset = keysOffset + keysArraySize

    writeMapFixedHeader(data, mapSize, headerOffset, arraysOffset)
    writeNumber(data, keysArraySize, arraysOffset)
    writeArray(data, map.keySet.toArray[Byte], keysOffset)
    writeNestedArray(data, map.values.toArray[Array[Short]], valuesOffset)
  }

  def convertToRow(contig: String, start: Int, end: Int, bases: String, cov: Short, refCount: Short, altsCount: Short,
                   altsMap: Map[Byte, Short], qualsMap: Map[Byte, Array[Short]]): UnsafeRow = {
    val nullRegionLen, fixedRegionIndex = 8
    val numFields = 9 //FIXME constant fields num
    val fixedRegionLen = numFields * wordSize
    val varRegionLen = roundUp(contig.length, wordSize) + roundUp(bases.length,wordSize)
    val varRegionIndex = nullRegionLen + fixedRegionLen
    val altsMapSize = if (altsMap == null) 0 else calculateAltsMapSizes(altsMap)._1
    val qualMapSize = if(qualsMap == null) 0 else calculateQualMapSizes(qualsMap)._1

    val dataSize = nullRegionLen + fixedRegionLen + varRegionLen + altsMapSize + qualMapSize

    val data = new Array[Byte](dataSize)
    val altsMapElementsOffset  = varRegionIndex + roundUp(contig.length, wordSize) + roundUp(bases.length, wordSize)

    val qualMapElementsOffset = if (altsMap == null) altsMapElementsOffset
    else altsMapElementsOffset + calculateAltsMapSizes(altsMap)._1

    writeString(data, contig, fixedRegionIndex + 0 * wordSize, varRegionIndex, contigByteMap(contig))
    writeNumber(data, start, fixedRegionIndex + 1 * wordSize)
    writeNumber(data, end, fixedRegionIndex + 2 * wordSize)
    writeString(data, bases, fixedRegionIndex + 3 * wordSize, varRegionIndex + roundUp(contig.length,wordSize), getBytesForSequence(bases))
    writeNumber(data, cov, fixedRegionIndex + 4 * wordSize)
    writeNumber(data, refCount, fixedRegionIndex + 5 * wordSize)
    writeNumber(data, altsCount, fixedRegionIndex + 6 * wordSize)

    if (altsMap != null)
      writeMap(data,altsMap, fixedRegionIndex + 7*wordSize, altsMapElementsOffset)

    if (qualsMap != null)
      writeQualsMap(data,qualsMap, fixedRegionIndex + 8*wordSize, qualMapElementsOffset)

    val row = new UnsafeRow(numFields)
    row.pointTo(data, data.length)
    if (altsMap == null)
      row.setNullAt(7)
    if (qualsMap == null)
      row.setNullAt(8)
    row
  }
}
