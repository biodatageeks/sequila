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
      condition.head match {
        case And(LessThanOrEqual(l1, g1), LessThanOrEqual(l2, g2)) =>
          Some((joinType,
            getKeys(l1,l2,g1,g2,left,right),
            left, right))
        case  And(GreaterThanOrEqual(g1, l1), LessThanOrEqual(l2, g2)) =>
          Some((joinType,
            getKeys(l1,l2,g1,g2,left,right),
            left, right))
        case  And(LessThanOrEqual(l1, g1), GreaterThanOrEqual(g2, l2)) =>
          Some((joinType,
            getKeys(l1,l2,g1,g2,left,right),
            left, right))
        case  And(GreaterThanOrEqual(g1, l1), GreaterThanOrEqual(g2, l2)) =>
          Some((joinType,
            getKeys(l1,l2,g1,g2,left,right),
            left, right))
        case _ => None
      }
    case _ =>
      None
  }

  def getKeys(l1:Expression,l2:Expression,g1:Expression,g2:Expression,left:LogicalPlan,right:LogicalPlan): Seq[Expression] ={
    var leftStart:Expression = null
    var leftEnd:Expression = null
    var rightStart:Expression = null
    var rightEnd:Expression = null
    if (canEvaluate(g1, right)) {
      if (canEvaluate(l1, left)) {
        leftStart=l1
        leftEnd=g2
        rightStart=l2
        rightEnd=g1
      } else {
        leftStart=l2
        leftEnd=g2
        rightStart=l1
        rightEnd=g1
      }
    } else {
      if (canEvaluate(l1, left)) {
        leftStart=l1
        leftEnd=g1
        rightStart=l2
        rightEnd=g2
      } else {
        leftStart=l2
        leftEnd=g1
        rightStart=l1
        rightEnd=g2
      }
    }
    List(leftStart, leftEnd, rightStart, rightEnd).toSeq
  }
}