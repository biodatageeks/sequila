package org.biodatageeks.datasources.VCF

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._


class VCFRelation(path: String)(@transient val sqlContext: SQLContext) extends BaseRelation
  with PrunedScan
  with Serializable
  with Logging{

  val spark = sqlContext.sparkSession


  lazy val df =  spark.read
    .format("com.lifeomic.variants")
    .option("use.format.type", "false")
    .load(path)
    .withColumnRenamed("sampleid","sample_id")
    .withColumnRenamed("chrom", "contig")




  override def schema: org.apache.spark.sql.types.StructType = {
   df.schema
  }

  override def buildScan(requiredColumns: Array[String] ) = {

    {
      if (requiredColumns.length > 0)
        df.select(requiredColumns.head, requiredColumns.tail: _*)
      else
        df
    }.rdd


  }

}
