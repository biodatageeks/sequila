package org.biodatageeks.sequila.pileup.converters

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.utils.{Columns, DataQualityFuncs}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class PileupConverter (spark: SparkSession) extends Serializable {

  private object Indices {
    val contig = 0
    val position = 1
    val ref = 2
    val cov = 3
    val pileupString = 4
    val qualityString = 5
    val altsMap = 6
    val qualsMap = 7
  }

  val rawPileupCol = "raw_pileup"
  val rawQualityCol = "raw_quality"


  def transformSamtoolsResult (df:DataFrame): DataFrame = {
    val dfMap = generateAltMap(df)
    val dfBlocks = generateCompressedOutput(dfMap)
    dfBlocks
  }

  def removeDeletedBases(pileup: String, quality:String): (String, String) = {
    if (!pileup.contains('*'))
      return (pileup, quality)

    val pilBuf = new StringBuilder()
    val qualBuf = new StringBuilder()

    var i = 0
    while (i<pileup.length){
      if(pileup(i) != '*') {
        pilBuf.append(pileup(i))
        qualBuf.append(quality(i))
      }
      i += 1
    }

    (pilBuf.toString, qualBuf.toString)
  }


  def generateQualityMap(rawPileup: String, qualityString: String, ref: String): mutable.Map[Byte, mutable.HashMap[String, Short]] = {
    val cleanPileup = PileupStringUtils.removeAllMarks(rawPileup)
    val (pileup, quality) = removeDeletedBases(cleanPileup, qualityString)
    assert (pileup.length == quality.length)

    val refPileup = pileup.replace(PileupStringUtils.refMatchPlusStrand, ref.charAt(0)).replace(PileupStringUtils.refMatchMinuStrand, ref.charAt(0))
    val res = new mutable.HashMap[Byte, mutable.HashMap[String, Short]]

    for(index <- refPileup.indices){
      val pileupChar = refPileup(index)
      val qualityChar = quality(index)
      val baseMap = res.getOrElse(pileupChar.toByte, new mutable.HashMap[String, Short]())
      val baseQualCount = baseMap.getOrElse(qualityChar.toString, 0.toShort)
      baseMap.update(qualityChar.toString, (baseQualCount+1).toShort)
      res.update(pileupChar.toByte, baseMap)
    }

    res
  }

  def generateAltMap(df: DataFrame):DataFrame = {
    import spark.implicits._

    val delContext = new DelContext()
//
    val dataMapped = df.map(row => {
      val contig = DataQualityFuncs.cleanContig(row.getString(Indices.contig))
      val position = row.getInt(Indices.position)
      val ref = row.getString(Indices.ref).toUpperCase()
      val rawPileup = row.getString(Indices.pileupString)
      val qualityString = row.getString(Indices.qualityString)

      //FIXME - needed manual escaping in pileup file ... maybe can be done in better way...
     val properQualityString = StringUtils.replace(qualityString,"\\\"", "\"")

      val pileup = PileupStringUtils.removeStartAndEndMarks(rawPileup)
      val basesCount = PileupStringUtils.getBaseCountMap(pileup)
      val map = mutable.Map.empty[Byte, Short]

      if (pileup.contains("+") || pileup.contains("-")) {
        val indelPattern = "[\\+\\-]([0-9]+)\\^?([A-Za-z]+)?".r
        val numberPattern = "([0-9]+)".r

        val matches = indelPattern.findAllMatchIn(pileup)
        while (matches.hasNext) {
          val indelSign = matches.next().toString()
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
      val cov = (row.getShort(Indices.cov) - diff).toShort

      val qMap = if (map.nonEmpty) generateQualityMap(rawPileup,properQualityString, ref ) else null

      (contig, position, ref , cov, rawPileup, qualityString, if (map.nonEmpty) map else null, qMap)
    })
    dataMapped.toDF(Columns.CONTIG, Columns.START, Columns.REF, Columns.COVERAGE, rawPileupCol, rawQualityCol, Columns.ALTS, "quals")
  }

  def generateCompressedOutput(df: DataFrame):DataFrame = {
    import spark.implicits._
    val dataRdd = df.rdd
    var blockLength, i, cov, prevCov = 0
    val prevAlt = mutable.Map.empty[Byte, Short]
    val prevQual = mutable.Map.empty[Byte, Map[String, Short]]
    var curBase, curContig, prevContig = ""
    val buffer = new StringBuilder()
    val arr = new ArrayBuffer[(String, Int, Int, String, Short, Map[Byte, Short], Map[Byte, Map[String, Short]] )]()
    var positionStart = dataRdd.first().getInt(1)
    var curPosition = positionStart
    val size = dataRdd.count()
    var rowCounter = 0


    for (row <- dataRdd.collect()) {
      curContig = row.getString(Indices.contig)
      curPosition = row.getInt(Indices.position)
      curBase = row.getString(Indices.ref)
      cov = row.getShort(Indices.cov)
      val currAlt = row.getMap[Byte, Short](Indices.altsMap)
      val currQualityMap = row.getMap[Byte, Map[String, Short]](Indices.qualsMap)

      if (prevContig.nonEmpty && prevContig != curContig) {
        if (prevAlt.nonEmpty)
          arr.append((prevContig, positionStart + i, positionStart + i, buffer.toString(), prevCov.toShort, prevAlt.toMap, prevQual.toMap))
        else
          arr.append((prevContig, positionStart + i - blockLength, positionStart + i - 1, buffer.toString(), prevCov.toShort, null,null))
        blockLength = 0
        positionStart = row.getInt(1)
        i = 0
        buffer.clear()
        prevAlt.clear()
        prevQual.clear()
      }
      else {
        var position = positionStart + i
        var gapLen = curPosition - position
        if (gapLen != 0) { // there is a gap in coverage
          if (prevAlt.nonEmpty)
            arr.append((prevContig, positionStart + i, positionStart + i -1, buffer.toString(), prevCov.toShort, prevAlt.toMap,prevQual.toMap))
          else
            arr.append((prevContig, positionStart + i - blockLength, positionStart + i -1, buffer.toString(), prevCov.toShort,null,null))
          if (currAlt !=null) {
            currAlt.foreach { case (k, v) => prevAlt(k) = v } // prevalt equals curAlt
            currQualityMap.foreach { case (k, v) => prevQual(k) = v }
          }
          buffer.clear()
          blockLength = 0
          i += gapLen
        } else if (prevAlt.nonEmpty) {
          arr.append((curContig, positionStart + i - 1, positionStart + i - 1, buffer.toString(), prevCov.toShort, prevAlt.toMap,prevQual.toMap))
          blockLength = 0
          prevAlt.clear()
          prevQual.clear()
          buffer.clear()
          if (currAlt !=null) {
            currAlt.foreach { case (k, v) => prevAlt(k) = v } // prevalt equals curAlt
            currQualityMap.foreach { case (k, v) => prevQual(k) = v }
          }
        }
        else if (currAlt != null) { // there is ALT in this posiion
          if (blockLength != 0) { // there is previous group -> convert it
            arr.append((curContig, positionStart + i - blockLength, positionStart + i - 1, buffer.toString(), prevCov.toShort,null,null))
            buffer.clear()
            blockLength = 0
            currAlt.foreach { case (k, v) => prevAlt(k) = v } // prevalt equals curAlt
            currQualityMap.foreach { case (k, v) => prevQual(k) = v }

          } else if (blockLength == 0) {
            currAlt.foreach { case (k, v) => prevAlt(k) = v }// prevalt equals curAlt
            currQualityMap.foreach { case (k, v) => prevQual(k) = v }

          }
        } else if (cov != 0 && prevCov >= 0 && prevCov != cov && i > 0) {
          arr.append((curContig, positionStart + i - blockLength, positionStart + i - 1, buffer.toString(), prevCov.toShort, null,null))
          buffer.clear()
          blockLength = 0
        }
      }

      prevCov = cov
      prevContig = curContig
      buffer.append(curBase)
      blockLength += 1
      if (rowCounter == size-1)
        arr.append((curContig, positionStart + i - blockLength+1, positionStart + i , buffer.toString(), prevCov.toShort,null,null))

      i += 1; rowCounter +=1
    }
    arr.toDF(Columns.CONTIG, Columns.START, Columns.END, Columns.REF, Columns.COVERAGE, Columns.ALTS, "quals")
  }

}
