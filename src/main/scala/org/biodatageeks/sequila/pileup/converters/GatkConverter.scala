package org.biodatageeks.sequila.pileup.converters

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.utils.{Columns, DataQualityFuncs, UDFRegister}
import scala.collection.mutable
import org.apache.spark.sql.functions._

class GatkConverter(spark: SparkSession) extends Serializable {

  def transformToCommonFormat(df:DataFrame, caseSensitive:Boolean): DataFrame = {
    UDFRegister.register(spark)
    val dfMap = generateAltsQuals(df, caseSensitive)
    val dfStringMap = dfMap
      .withColumn(s"${Columns.ALTS}", expr(s"alts_to_char(${Columns.ALTS})"))
      .withColumn(s"${Columns.QUALS}", expr(s"quals_to_char(${Columns.QUALS})"))
    dfStringMap
  }

  def generateAltsQuals(df: DataFrame, caseSensitive: Boolean):DataFrame = {
    import spark.implicits._
//    val delContext = new DelContext()

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
//      val diff = delContext.getDelTransferForLocus(contig, position)
      val cov = row.getShort(GatkSchema.cov)
      (contig, position, ref , cov, pileup, if (map.nonEmpty) map else null)

    })
    dataMapped.toDF(Columns.CONTIG, Columns.START, Columns.REF, Columns.COVERAGE, Columns.ALTS, Columns.QUALS)
  }

}
