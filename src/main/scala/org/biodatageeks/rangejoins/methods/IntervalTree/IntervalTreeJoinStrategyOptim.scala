package org.biodatageeks.rangejoins.IntervalTree

import org.biodatageeks.rangejoins.common.ExtractRangeJoinKeys
//import org.biodatageeks.rangejoins.methods.genApp.IntervalTreeJoin
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}

/**
  * Created by marek on 27/01/2018.
  */
class IntervalTreeJoinStrategyOptim(spark: SparkSession) extends Strategy with Serializable with  PredicateHelper {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractRangeJoinKeys(joinType, rangeJoinKeys, left, right) =>
      IntervalTreeJoinOptim(planLater(left), planLater(right), rangeJoinKeys, spark) :: Nil
    case _ =>
      Nil
  }
}
