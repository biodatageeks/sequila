package org.biodatageeks.sequila.rangejoins.genApp

import org.biodatageeks.sequila.rangejoins.common.{ExtractRangeJoinKeys, ExtractRangeJoinKeysWithEquality}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}
import org.biodatageeks.sequila.rangejoins.methods.genApp.IntervalTreeJoinChromosome

class IntervalTreeJoinStrategy(spark: SparkSession) extends Strategy with Serializable with  PredicateHelper {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractRangeJoinKeys(joinType, rangeJoinKeys, left, right) =>
      IntervalTreeJoin(planLater(left), planLater(right), rangeJoinKeys, spark) :: Nil
    case ExtractRangeJoinKeysWithEquality(joinType, rangeJoinKeys, left, right,condition) =>
      IntervalTreeJoinChromosome(planLater(left), planLater(right), rangeJoinKeys, spark, condition) :: Nil
    case _ =>
      Nil
  }
}
