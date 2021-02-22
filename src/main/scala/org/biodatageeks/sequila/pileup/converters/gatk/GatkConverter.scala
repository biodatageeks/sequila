package org.biodatageeks.sequila.pileup.converters.gatk

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.pileup.converters.{CommonPileupFormat, PileupStringUtils}
import org.biodatageeks.sequila.utils.{DataQualityFuncs, UDFRegister}

import scala.collection.mutable

class GatkConverter(spark: SparkSession) extends Serializable {

  def transformToCommonFormat(df:DataFrame, caseSensitive:Boolean): DataFrame = {
    val dfMap = generateAltsQuals(df, caseSensitive)
    dfMap
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
    spark.createDF(dataMapped.collect().toList, CommonPileupFormat.schemaQualsMap.fields.toList )
  }

}
