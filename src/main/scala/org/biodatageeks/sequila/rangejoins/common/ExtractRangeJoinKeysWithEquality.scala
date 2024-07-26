package org.biodatageeks.sequila.rangejoins.common

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.Inner

/**
 * A pattern that finds joins with equality conditions that can be evaluated using range-join.
 */

object ExtractRangeJoinKeysWithEquality extends Logging with  PredicateHelper {

  //joinType, rangeJoinKeys, left, right, condition
  //(JoinType, Seq[Expression], LogicalPlan, LogicalPlan)
  type ReturnType =
    (JoinType, Seq[Expression], LogicalPlan, LogicalPlan, Option[Expression])
  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case join @ Join(left, right, joinType, condition, hint) =>
      logDebug(s"Considering join on: $condition")
      val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)
//      predicates.foreach(r=>println(r.simpleString))
      /* Look for expressions a < b and c < d where a,b and c,d belong to the same LogicalPlan
    **/
      //println(condition.head)
      if (condition.size!=0 && joinType == Inner) {
        condition.head match {
          case And(And(EqualTo(l3, r3), LessThanOrEqual(l1, g1)), LessThanOrEqual(l2, g2)) =>
            val rangeJoinKeys = getKeys(l1, l2, g1, g2, l3, r3, left, right)
            Some((joinType, rangeJoinKeys, left, right, condition))
          case And(And(EqualTo(l3, r3), GreaterThanOrEqual(g1, l1)), LessThanOrEqual(l2, g2)) =>
            Some((joinType,
              getKeys(l1, l2, g1, g2, l3, r3, left, right),
              left, right,condition))
          case And(And(EqualTo(l3, r3), LessThanOrEqual(l1, g1)), GreaterThanOrEqual(g2, l2)) =>
            Some((joinType,
              getKeys(l1, l2, g1, g2, l3, r3, left, right),
              left, right, condition))
          case And(And(EqualTo(l3, r3), GreaterThanOrEqual(g1, l1)), GreaterThanOrEqual(g2, l2)) =>
            Some((joinType,
              getKeys(l1, l2, g1, g2, l3, r3, left, right),
              left, right, condition))
          case _ => None
        }
      } else {
        None
      }
    case _ =>
      None
  }

  def getKeys(l1:Expression,l2:Expression,g1:Expression,g2:Expression,l3:Expression,r3:Expression,left:LogicalPlan,right:LogicalPlan): Seq[Expression] ={
    var leftStart:Expression = null
    var leftEnd:Expression = null
    var rightStart:Expression = null
    var rightEnd:Expression = null
    var leftEquality:Expression = null
    var rightEquality:Expression = null

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

    if (canEvaluate(l3, left)) {
      leftEquality = l3
      rightEquality = r3
    } else {
      leftEquality = r3
      rightEquality = l3
    }

    List(leftStart, leftEnd, rightStart, rightEnd,leftEquality,rightEquality)
  }
}