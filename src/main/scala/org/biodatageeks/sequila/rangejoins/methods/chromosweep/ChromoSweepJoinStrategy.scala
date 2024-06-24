package org.biodatageeks.sequila.rangejoins.methods.chromosweep

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}
import org.biodatageeks.sequila.rangejoins.common.ExtractRangeJoinKeys

class ChromoSweepJoinStrategy(spark: SparkSession) extends Strategy with Serializable with  PredicateHelper {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractRangeJoinKeys(joinType, rangeJoinKeys, left, right) =>
     ChromoSweepJoin(planLater(left), planLater(right), rangeJoinKeys, spark,left,right) :: Nil
    case _ => Nil
  }
}
