package org.biodatageeks.sequila.rangejoins.IntervalTree

import org.biodatageeks.sequila.rangejoins.common.{ExtractRangeJoinKeys, ExtractRangeJoinKeysWithEquality}
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeJoinOptimChromosome
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}

import scala.annotation.tailrec

/**
  * Created by marek on 27/01/2018.
  */
class IntervalTreeJoinStrategyOptim(spark: SparkSession) extends Strategy with Serializable with  PredicateHelper {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractRangeJoinKeys(joinType, rangeJoinKeys, left, right) =>
      IntervalTreeJoinOptim(planLater(left), planLater(right), rangeJoinKeys, spark,left,right) :: Nil
    case ExtractRangeJoinKeysWithEquality(joinType, rangeJoinKeys, left, right) => {
      val minOverlap = spark.sqlContext.getConf("spark.biodatageeks.rangejoin.minOverlap","1")
      val maxGap = spark.sqlContext.getConf("spark.biodatageeks.rangejoin.maxGap","0")
      //val maxBroadcastSize = spark.sqlContext.getConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (10*(1024*1024)).toString)
      val useJoinOrder = spark.sqlContext.getConf("spark.biodatageeks.rangejoin.useJoinOrder","false")
      /*register functions*/
      IntervalTreeJoinOptimChromosome(planLater(left), planLater(right),
        rangeJoinKeys, spark, minOverlap.toInt, maxGap.toInt, useJoinOrder.toBoolean) :: Nil
    }
    case _ =>
      Nil
  }


}
