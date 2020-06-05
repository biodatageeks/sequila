package org.biodatageeks.sequila.pileup.converters

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.utils.{Columns, DataQualityFuncs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class PileupConverter (spark: SparkSession) {

  def transformSamtoolsResult (df:DataFrame): DataFrame = {
    val dfMap = generateAltMap(df)
    val dfBlocks = generateCompressedOutput(dfMap)
    dfBlocks
  }

  def generateAltMap (df: DataFrame):DataFrame = {
    import spark.implicits._

    val delContext = new DelContext()
//
    val dataMapped = df.map(row => {
      val contig = DataQualityFuncs.cleanContig(row.getString(0))
      val position = row.getInt(1)
      val rawPileup = row.getString(4).toUpperCase
      val splitted = rawPileup.split('^')
      val buffer = new StringBuilder()
      var i = 0

      if (contig=="1" && position == 6272)
        println()

//      while (i < splitted.length) {
//        if (i == 0 && splitted(i).nonEmpty)
//          buffer.append(splitted(i))
//        else if (i == 0 && splitted(i).isEmpty)
//          i += 1
//        else if (i > 0 && splitted(i).nonEmpty)
//          buffer.append(splitted(i).substring(1))
//        i += 1
//      }

      while (i < splitted.length) {
        if (i == 0)
          buffer.append(splitted(i))
        else if (i > 0 && splitted(i).nonEmpty)
          buffer.append(splitted(i).substring(1))
        i += 1
      }


      val pileup = buffer.toString()
      val map = mutable.Map.empty[Byte, Short]
      val basesCount = new mutable.HashMap[String, Int]()
      basesCount("A") = pileup.count(_ == 'A')
      basesCount("C") = pileup.count(_ == 'C')
      basesCount("G") = pileup.count(_ == 'G')
      basesCount("T") = pileup.count(_ == 'T')
      basesCount("N") = pileup.count(_ == 'N')


      if (pileup.contains("+") || pileup.contains("-")) {
        val indelPattern = "[\\+\\-]([0-9]+)\\^?([A-Za-z]+)?".r
        val numberPattern = "([0-9]+)".r

        val matches = indelPattern.findAllMatchIn(pileup)
        while (matches.hasNext) {
          val indelSign = matches.next().toString().toUpperCase()
          val indel = indelSign.substring(1)
          val indelLenStr = numberPattern.findFirstIn(indel).get
          val indelLen = indelLenStr.toInt
          val bases = indel.substring(indelLenStr.length)
          val insertionBases = bases.substring(0, indelLen)

          if(indelSign.contains('-'))
            delContext.add(DelTransfer(contig, position, indelLen))

          basesCount.foreach {
            case (k, v) =>
              basesCount(k) = v - insertionBases.count(_ == k.charAt(0))
          }
        }
      }

      basesCount.foreach { case (k, v) =>
        if (v != 0)
          map += (k.charAt(0).toByte) -> (v.toShort)
      }

      val diff = delContext.getDelTransferForLocus(contig, position)
      val cov = (row.getShort(3) - diff).toShort
      (contig, position, row.getString(2).toUpperCase(), cov, if (map.nonEmpty) map else null)
    })
    dataMapped.toDF(Columns.CONTIG, Columns.START, Columns.REF, Columns.COVERAGE, Columns.ALTS)
  }

  def generateCompressedOutput(df: DataFrame):DataFrame = {
    import spark.implicits._
    val dataRdd = df.rdd
    var blockLength, i, cov, prevCov = 0
    var prevAlt = mutable.Map.empty[Byte, Short]
    var curBase, curContig, prevContig = ""
    val buffer = new StringBuilder()
    val arr = new ArrayBuffer[(String, Int, Int, String, Short, Map[Byte, Short])]()
    var positionStart = dataRdd.first().getInt(1)
    var curPosition = positionStart
    //    var prevPosition = positionStart -1
    val size = dataRdd.count()
    var rowCounter = 0


    for (row <- dataRdd.collect()) {
      cov = row.getShort(3)
      curBase = row.getString(2)
      curContig = row.getString(0)
      val currAlt = row.getMap[Byte, Short](4)
      curPosition = row.getInt(1)

      if (prevContig.nonEmpty && prevContig != curContig) {
        if (prevAlt.nonEmpty)
          arr.append((prevContig, positionStart + i, positionStart + i, buffer.toString(), prevCov.toShort, prevAlt.toMap))
        else
          arr.append((prevContig, positionStart + i - blockLength, positionStart + i - 1, buffer.toString(), prevCov.toShort, null))
        blockLength = 0
        positionStart = row.getInt(1)
        i = 0
        buffer.clear()
        prevAlt.clear()
      }
      else {
        var position = positionStart + i
        var gapLen = curPosition - position
        if (gapLen != 0) { // there is a gap in coverage
          if (prevAlt.nonEmpty)
            arr.append((prevContig, positionStart + i, positionStart + i -1, buffer.toString(), prevCov.toShort, prevAlt.toMap))
          else
            arr.append((prevContig, positionStart + i - blockLength, positionStart + i -1, buffer.toString(), prevCov.toShort, null))
          if (currAlt !=null)
            currAlt.foreach { case (k, v) => prevAlt(k) = v } // prevalt equals curAlt
          buffer.clear()
          blockLength = 0
          i += gapLen
        } else if (prevAlt.nonEmpty) {
          arr.append((curContig, positionStart + i - 1, positionStart + i - 1, buffer.toString(), prevCov.toShort, prevAlt.toMap))
          blockLength = 0
          prevAlt.clear()
          buffer.clear()
          if (currAlt !=null)
            currAlt.foreach { case (k, v) => prevAlt(k) = v } // prevalt equals curAlt
        }
        else if (currAlt != null) { // there is ALT in this posiion
          if (blockLength != 0) { // there is previous group -> convert it
            arr.append((curContig, positionStart + i - blockLength, positionStart + i - 1, buffer.toString(), prevCov.toShort, null))
            buffer.clear()
            blockLength = 0
            currAlt.foreach { case (k, v) => prevAlt(k) = v } // prevalt equals curAlt
          } else if (blockLength == 0) {
            currAlt.foreach { case (k, v) => prevAlt(k) = v }// prevalt equals curAlt
          }
        } else if (cov != 0 && prevCov >= 0 && prevCov != cov && i > 0) {
          arr.append((curContig, positionStart + i - blockLength, positionStart + i - 1, buffer.toString(), prevCov.toShort, null))
          buffer.clear()
          blockLength = 0
        }
      }

      prevCov = cov
      prevContig = curContig
      buffer.append(curBase)
      blockLength += 1
      if (rowCounter == size-1)
        arr.append((curContig, positionStart + i - blockLength+1, positionStart + i , buffer.toString(), prevCov.toShort, null))

      i += 1; rowCounter +=1
    }
    arr.toDF(Columns.CONTIG, Columns.START, Columns.END, Columns.REF, Columns.COVERAGE, Columns.ALTS)
  }

}
