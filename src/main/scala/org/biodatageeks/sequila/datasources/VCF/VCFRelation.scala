package org.biodatageeks.sequila.datasources.VCF

import io.projectglow.Glow
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.biodatageeks.sequila.utils.{Columns, DataQualityFuncs}
import org.apache.spark.sql.functions._




class VCFRelation(path: String,
                  normalization_mode: Option[String] = None,
                  ref_genome_path : Option[String] = None )(@transient val sqlContext: SQLContext) extends BaseRelation
  with PrunedScan
  with Serializable
  with Logging {

  val spark: SparkSession = sqlContext.sparkSession

  val cleanContigUDF = udf[String, String](DataQualityFuncs.cleanContig)

  lazy val inputDf: DataFrame = spark
    .read
    .format("vcf")
    .load(path)
  lazy val dfNormalized = {
    normalization_mode match {
    case Some(m) => {
      if (m.equalsIgnoreCase("normalize") || m.equalsIgnoreCase("split_and_normalize")
        && ref_genome_path == None) throw new Exception(s"Variant normalization mode specified but ref_genome_path is empty ")
      Glow.transform(m.toLowerCase(), inputDf, Map("reference_genome_path" -> ref_genome_path.get))
    }
    case _ => inputDf
    }
  }.withColumnRenamed("contigName", Columns.CONTIG)
    .withColumnRenamed("start", Columns.START)
    .withColumnRenamed("end", Columns.END)
    .withColumnRenamed("referenceAllele", Columns.REF)
    .withColumnRenamed("alternateAlleles", Columns.ALT)

  lazy val df = dfNormalized
    .withColumn(Columns.CONTIG, cleanContigUDF(dfNormalized(Columns.CONTIG)))

  override def schema: org.apache.spark.sql.types.StructType = {
   df.schema
  }

  override def buildScan(requiredColumns: Array[String] ): RDD[Row] = {

    {
      if (requiredColumns.length > 0)
        df.select(requiredColumns.head, requiredColumns.tail: _*)
      else
        df
    }.rdd


  }

}
