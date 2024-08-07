package org.biodatageeks.sequila.pileup.converters.gatk

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.pileup.PileupReader
import org.biodatageeks.sequila.pileup.converters.common.{CommonPileupFormat, PileupConverter}
import org.biodatageeks.sequila.pileup.converters.samtools.PileupStringUtils
import org.biodatageeks.sequila.utils.{Columns, DataQualityFuncs}

import scala.collection.mutable

class GatkConverter(spark: SparkSession) extends Serializable with PileupConverter {

  override def transform(path: String): DataFrame = {
    val df = PileupReader.load(spark, path, GatkSchema.schema, delimiter = " ")
    toCommonFormat(df, caseSensitive = true )
  }
  def toCommonFormat(df:DataFrame, caseSensitive:Boolean): DataFrame = {
    val dfMap = generateAltsQuals(df, caseSensitive)
    val dfString = mapColumnsAsStrings(dfMap)
    dfString.orderBy(Columns.CONTIG, Columns.START)
  }

  def generateAltsQuals(df: DataFrame, caseSensitive: Boolean):DataFrame = {
    import spark.implicits._

    val dataMapped = df.map(row => {
      val contig = DataQualityFuncs.cleanContig(row.getString(GatkSchema.contig))
      val position = row.getInt(GatkSchema.position)
      val ref = row.getString(GatkSchema.ref).toUpperCase()
      val pileup = if (caseSensitive)
        row.getString(GatkSchema.pileupString)
      else
        row.getString(GatkSchema.pileupString).toUpperCase()

      val basesCount = PileupStringUtils.getBaseCountMap(pileup)
      val map = mutable.Map.empty[Byte, Short]

      basesCount.foreach { case (k, v) =>
        if (v != 0)
          map += (k.charAt(0).toByte) -> (v.toShort)
      }

      val cov = pileup.length.toShort
      (contig, position, position, ref , cov, if (map.nonEmpty) map else null, null)

    })
    spark.createDF(dataMapped.collect().toList, CommonPileupFormat.schemaAltsQualsMap.fields.toList )
  }
}
