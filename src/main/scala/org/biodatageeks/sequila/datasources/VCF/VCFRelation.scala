package org.biodatageeks.sequila.datasources.VCF

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.biodatageeks.sequila.utils.Columns


class VCFRelation(path: String)(@transient val sqlContext: SQLContext) extends BaseRelation
  with PrunedScan
  with Serializable
  with Logging{

  val spark: SparkSession = sqlContext.sparkSession


  lazy val df: DataFrame =  spark.read
    .format("com.lifeomic.variants")
    .option("use.format.type", "false")
    .load(path)
    .withColumnRenamed("sampleid", Columns.SAMPLE)
    .withColumnRenamed("chrom", Columns.CONTIG)




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
