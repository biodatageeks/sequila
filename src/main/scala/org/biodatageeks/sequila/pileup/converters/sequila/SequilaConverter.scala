package org.biodatageeks.sequila.pileup.converters.sequila

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.pileup.PileupReader
import org.biodatageeks.sequila.pileup.converters.common.{CommonPileupFormat, PileupConverter}
import org.biodatageeks.sequila.utils.{AlignmentConstants, Columns}


class SequilaConverter (spark: SparkSession) extends Serializable with PileupConverter {

  override def transform(path: String): DataFrame = {
    val df = PileupReader.load(spark, path, CommonPileupFormat.schemaAltsQualsString, delimiter = "|")
    toCommonFormat(df, caseSensitive = true)
  }

  def toCommonFormat(df:DataFrame, caseSensitive:Boolean): DataFrame = {
    val dfMap = generatePerBaseOutput(df, caseSensitive)
    dfMap.orderBy(Columns.CONTIG, Columns.START)
  }

  def generatePerBaseOutput(df: DataFrame, caseSensitive: Boolean): DataFrame = {
    val altsIncluded = df.schema.length >= 6
    val qualsIncluded = df.schema.length >= 7
    val perBase = df.rdd.flatMap { r => {
        val chr = r.getString(SequilaSchema.contig)
        val start = r.getInt(SequilaSchema.position_start)
        val end = r.getInt(SequilaSchema.position_end) + 1
        val ref = r.getString(SequilaSchema.ref)
        val cov = r.getShort(SequilaSchema.cov)

        val altsMap = if (altsIncluded) r.getString(SequilaSchema.altsMap) else null
        val qualsMap = if (qualsIncluded) r.getString(SequilaSchema.qualsMap) else null

        val array = new Array[(String, Int, Int, String, Short, String, String)](end-start)

        var cnt = 0
        var position = start

        while (position < end) {
          if (ref==AlignmentConstants.REF_SYMBOL)
            array(cnt) = (chr, position, position, "R", cov, altsMap, qualsMap)
            else
            array(cnt) = (chr, position, position, ref.substring(cnt, cnt + 1), cov, altsMap, qualsMap)
          cnt +=1
          position+=1
        }
        array
      }
      }
    spark.createDF(perBase.collect().toList, CommonPileupFormat.schemaAltsQualsString.fields.toList )
  }

}
