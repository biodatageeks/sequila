package genApp

import genApp.ExtractRangeJoinKeys.{canEvaluate, logDebug, splitConjunctivePredicates}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}

class IntervalTreeJoinStrategy(spark: SparkSession) extends Strategy with Serializable with  PredicateHelper {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractRangeJoinKeys(joinType, rangeJoinKeys, left, right) =>
      IntervalTreeJoin(planLater(left), planLater(right), rangeJoinKeys, spark) :: Nil
    case _ =>
      Nil
  }
}
