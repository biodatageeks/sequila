package genApp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
/**
 * A pattern that finds joins with equality conditions that can be evaluated using range-join.
 */

object ExtractRangeJoinKeys extends Logging with  PredicateHelper {
  type ReturnType =
    (JoinType, Seq[Expression], LogicalPlan, LogicalPlan)
  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case join @ Join(left, right, joinType, condition) =>
      logDebug(s"Considering join on: $condition")
      val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)
      /* Look for expressions a < b and c < d where a,b and c,d belong to the same LogicalPlan
    **/

      val leftExpressions = left.expressions.head
      val rightExpressions = right.expressions.head

      val leftRangeKeys = leftExpressions.flatMap(x => x match {
        case LessThanOrEqual(l, r) if (canEvaluate(l, left) && canEvaluate(r, left)) =>
          Some((l, r))
        case LessThan(l, r) if (canEvaluate(l, left) && canEvaluate(r, left)) =>
          Some((l, r))
        case GreaterThan(l, r) if (canEvaluate(l, left) && canEvaluate(r, left)) =>
          Some((r, l))
        case GreaterThanOrEqual(l, r) if (canEvaluate(l, left) && canEvaluate(r, left)) =>
          Some((r, l))
        case _ => None
      })
      val rightRangeKeys = rightExpressions.flatMap(x => x match {
        case LessThanOrEqual(l, r) if (canEvaluate(l, right) && canEvaluate(r, right)) =>
          Some((l, r))
        case LessThan(l, r) if (canEvaluate(l, right) && canEvaluate(r, right)) =>
          Some((l, r))
        case GreaterThan(l, r) if (canEvaluate(l, right) && canEvaluate(r, right)) =>
          Some((r, l))
        case GreaterThanOrEqual(l, r) if (canEvaluate(l, right) && canEvaluate(r, right)) =>
          Some((r, l))
        case _ => None
      })
      condition.head match {
        case Or(And(LessThanOrEqual(_, _), LessThanOrEqual(_, _)), And(LessThanOrEqual(_, _), LessThanOrEqual(_, _))) |
          Or(And(GreaterThanOrEqual(_, _), LessThanOrEqual(_, _)), And(LessThanOrEqual(_, _), LessThanOrEqual(_, _))) |
          Or(And(LessThanOrEqual(_, _), GreaterThanOrEqual(_, _)), And(LessThanOrEqual(_, _), LessThanOrEqual(_, _))) |
          Or(And(GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _)), And(LessThanOrEqual(_, _), LessThanOrEqual(_, _))) |
          Or(And(LessThanOrEqual(_, _), LessThanOrEqual(_, _)), And(GreaterThanOrEqual(_, _), LessThanOrEqual(_, _))) |
          Or(And(GreaterThanOrEqual(_, _), LessThanOrEqual(_, _)), And(GreaterThanOrEqual(_, _), LessThanOrEqual(_, _))) |
          Or(And(LessThanOrEqual(_, _), GreaterThanOrEqual(_, _)), And(GreaterThanOrEqual(_, _), LessThanOrEqual(_, _))) |
          Or(And(GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _)), And(GreaterThanOrEqual(_, _), LessThanOrEqual(_, _))) |
          Or(And(LessThanOrEqual(_, _), LessThanOrEqual(_, _)), And(LessThanOrEqual(_, _), GreaterThanOrEqual(_, _))) |
          Or(And(GreaterThanOrEqual(_, _), LessThanOrEqual(_, _)), And(LessThanOrEqual(_, _), GreaterThanOrEqual(_, _))) |
          Or(And(LessThanOrEqual(_, _), GreaterThanOrEqual(_, _)), And(LessThanOrEqual(_, _), GreaterThanOrEqual(_, _))) |
          Or(And(GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _)), And(LessThanOrEqual(_, _), GreaterThanOrEqual(_, _))) |
          Or(And(LessThanOrEqual(_, _), LessThanOrEqual(_, _)), And(GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _))) |
          Or(And(GreaterThanOrEqual(_, _), LessThanOrEqual(_, _)), And(GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _))) |
          Or(And(LessThanOrEqual(_, _), GreaterThanOrEqual(_, _)), And(GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _))) |
          Or(And(GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _)), And(GreaterThanOrEqual(_, _), GreaterThanOrEqual(_, _))) =>
          Some((joinType,
          List(leftRangeKeys.head._1, leftRangeKeys.head._2, rightRangeKeys.head._1, rightRangeKeys.head._2).toSeq,
          left, right))
        case _ => None
      }
    case _ =>
      None
  }
}