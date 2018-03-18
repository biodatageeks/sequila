package org.biodatageeks.datasources.ADAM

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim

class ADAMRelation (path:String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  val spark = sqlContext
    .sparkSession
  spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil

  private def parguet = spark.read.parquet(path)

  override def schema: org.apache.spark.sql.types.StructType = parguet.schema

  override def buildScan(requiredColumns: Array[String],  filters: Array[Filter]): RDD[Row] = parguet.rdd

}
