package org.biodatageeks.sequila.pileup.converters.sequila

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.pileup.PileupReader
import org.biodatageeks.sequila.pileup.converters.common.{CommonPileupFormat, PileupConverter}
import org.biodatageeks.sequila.utils.Columns


class SequilaConverter (spark: SparkSession) extends Serializable with PileupConverter {

  override def transform(path: String): DataFrame = {
    val df = PileupReader.load(spark, path, CommonPileupFormat.schemaAltsQualsString)
    toCommonFormat(df, caseSensitive = true)
  }

  def toCommonFormat(df:DataFrame, caseSensitive:Boolean): DataFrame = {
    val dfMap = generatePerBaseOutput(df, caseSensitive)
    dfMap.orderBy(Columns.CONTIG, Columns.START)
  }

  def generatePerBaseOutput(df: DataFrame, caseSensitive: Boolean): DataFrame = {
    val perBase = df.rdd.flatMap { r => {
        val chr = r.getString(SequilaSchema.contig)
        val start = r.getInt(SequilaSchema.position_start)
        val end = r.getInt(SequilaSchema.position_end) + 1
        val ref = r.getString(SequilaSchema.ref)
        val cov = r.getShort(SequilaSchema.cov)
        val altsMap = r.getString(SequilaSchema.altsMap)
        val qualsMap = r.getString(SequilaSchema.qualsMap)

        val array = new Array[(String, Int, Int, String, Short, String, String)](end-start)

        var cnt = 0
        var position = start

        while (position < end) {
          array(cnt) = (chr, position, position, ref.substring(cnt, cnt+1), cov, altsMap, qualsMap)
          cnt +=1
          position+=1
        }
        array
      }
      }
    spark.createDF(perBase.collect().toList, CommonPileupFormat.schemaAltsQualsString.fields.toList )
  }

}
