package org.biodatageeks.sequila.rangejoins.IntervalTree

import org.apache.spark.sql.catalyst.InternalRow

case class Interval[T <% Int](start: T, end: T) {
  def overlaps(other: Interval[T]): Boolean = {
    (end >= start) && (other.end >= other.start) &&
      (end >= other.start && start <= other.end)
  }
}

case class IntervalWithRow[T<% Int](start: T, end: T, row: InternalRow){
  def overlaps(other: IntervalWithRow[T]): Boolean = {
    (end >= start) && (other.end >= other.start) &&
      (end >= other.start && start <= other.end)
  }
}