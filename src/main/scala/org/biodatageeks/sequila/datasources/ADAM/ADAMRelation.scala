package org.biodatageeks.sequila.datasources.ADAM

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.biodatageeks.sequila.utils.Columns

class ADAMRelation(path: String)(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with PrunedFilteredScan
    with Serializable {

  val spark: SparkSession = sqlContext.sparkSession

  private def df: DataFrame = {
    spark.read.parquet(path).withColumnRenamed("contigName", Columns.CONTIG)

  }

  override def schema: org.apache.spark.sql.types.StructType = df.schema

  override def buildScan(requiredColumns: Array[String],
                         filters: Array[Filter]): RDD[Row] = {

    df.select(requiredColumns.map(col): _*).rdd

  }

}
