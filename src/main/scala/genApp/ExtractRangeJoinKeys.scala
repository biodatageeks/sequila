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
      /* Look for expressions a < c, b < d such that a, b belong to one LogicalPlan and
+       * c, d belong to the other.
+       */
      val heterogeneousKeys = predicates.flatMap(x => x match {
        case LessThanOrEqual(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
          (canEvaluate(l, right) && canEvaluate(r, left)) => Some((l, r))
        case LessThan(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
          (canEvaluate(l, right) && canEvaluate(r, left)) => Some((l, r))
        case GreaterThan(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
          (canEvaluate(l, right) && canEvaluate(r, left)) => Some((r, l))
        case GreaterThanOrEqual(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
          (canEvaluate(l, right) && canEvaluate(r, left)) => Some((r, l))
        case _ => None
      })
      /* Now verify that the condition does indeed describe an overlapping. The criteria are:
     * 1) There is exactly one element on the left and right rangeKeys
     * (let's call them (a,b) and (c,d) respectively)
     * 2) There are exectly two elements on the heterogeneousKeys and they are of the form of
     * * either ((a, c), (c, b)) or ((c, a), (a, d))
     **/

      (leftRangeKeys, rightRangeKeys, heterogeneousKeys) match{
        case (l, r, _) if (l.size != 1 || r.size != 1) => None
        case (_, _, h) if (h.size != 2) => None
        case (l, r, h) if ((h.contains((l.head._1, r.head._1)) //a-c, c-b
          && h.contains((r.head._1, l.head._2))) ||
          (h.contains((r.head._1, l.head._1)) && //c-a, a-d
            h.contains((l.head._1, r.head._2)))
          ) => Some((joinType,
          List(l.head._1, l.head._2, r.head._1, r.head._2).toSeq,
          left, right))
      }
    case _ =>
      None
  }
}