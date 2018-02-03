package org.biodatageeks.rangejoins.genApp

import org.biodatageeks.rangejoins.common.ExtractRangeJoinKeys
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}

class IntervalTreeJoinStrategy(spark: SparkSession) extends Strategy with Serializable with  PredicateHelper {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractRangeJoinKeys(joinType, rangeJoinKeys, left, right) =>
      IntervalTreeJoin(planLater(left), planLater(right), rangeJoinKeys, spark) :: Nil
    case _ =>
      Nil
  }
}
