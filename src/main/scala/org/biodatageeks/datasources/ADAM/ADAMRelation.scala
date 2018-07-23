package org.biodatageeks.datasources.ADAM

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.apache.spark.sql.functions.col

class ADAMRelation (path:String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with Serializable {

  val spark = sqlContext
    .sparkSession

  private def parquet = spark.read.parquet(path)

  override def schema: org.apache.spark.sql.types.StructType = parquet.schema

  override def buildScan(requiredColumns: Array[String],  filters: Array[Filter]): RDD[Row] = {
    requiredColumns.foreach(println(_))
    parquet
      .select(requiredColumns.map(col): _*)
      .rdd
  }

}
