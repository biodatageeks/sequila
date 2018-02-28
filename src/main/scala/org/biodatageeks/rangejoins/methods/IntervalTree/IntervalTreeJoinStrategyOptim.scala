package org.biodatageeks.rangejoins.IntervalTree

import org.biodatageeks.rangejoins.common.{ExtractRangeJoinKeys, ExtractRangeJoinKeysWithEquality}
import org.biodatageeks.rangejoins.methods.IntervalTree.IntervalTreeJoinOptimChromosome
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
      val minOverlap = spark.sqlContext.getConf("minOverlap","1")
      val maxGap = spark.sqlContext.getConf("maxGap","0")
      IntervalTreeJoinOptimChromosome(planLater(left), planLater(right),
        rangeJoinKeys, spark, left, right, minOverlap.toInt, maxGap.toInt) :: Nil
    }
    case _ =>
      Nil
  }


}
