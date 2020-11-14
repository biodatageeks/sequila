package org.biodatageeks.sequila.rangejoins.methods.chromosweep

case class SortedInterval[T](start: T, end: T)(implicit ev$1: T => Int) extends Ordered[SortedInterval[T]]{
  def haveStarted(point: T): Boolean = {
    start < point
  }
  def intersects(other: SortedInterval[T]) = {
    start <= other.end && end >= other.start
  }
  override def compare(that: SortedInterval[T]): Int =  if(start < that.start) -1 else if(start > that.start) 1 else 0
}
