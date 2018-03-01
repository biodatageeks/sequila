package org.biodatageeks.rangejoins.optimizer

object RangeJoinMethod extends Enumeration {
  type RangeJoinMethod = Value
  val JoinWithRowBroadcast, TwoPhaseJoin = Value
}
