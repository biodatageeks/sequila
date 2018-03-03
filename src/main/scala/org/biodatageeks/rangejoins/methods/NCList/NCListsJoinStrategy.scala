package org.biodatageeks.rangejoins.NCList

import org.biodatageeks.rangejoins.common.{ExtractRangeJoinKeys, ExtractRangeJoinKeysWithEquality}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}
import org.biodatageeks.rangejoins.methods.NCList.NCListsJoinChromosome

class NCListsJoinStrategy(spark: SparkSession) extends Strategy with Serializable with  PredicateHelper {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractRangeJoinKeys(joinType, rangeJoinKeys, left, right) =>
      NCListsJoin(planLater(left), planLater(right), rangeJoinKeys, spark) :: Nil
    case ExtractRangeJoinKeysWithEquality(joinType, rangeJoinKeys, left, right) =>
      NCListsJoinChromosome(planLater(left), planLater(right), rangeJoinKeys, spark) :: Nil
    case _ =>
      Nil
  }
}
