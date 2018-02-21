package org.biodatageeks.rangejoins.optimizer

object RangeJoinMethod extends Enumeration {
  type RangeJoinMethod = Value
  val JointWithRowBroadcast, TwoPhaseJoin = Value
}
