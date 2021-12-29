package org.biodatageeks.sequila.rangejoins.methods.chromosweep

case class RangeTreeNode(range: SortedInterval[Int], left: Option[RangeTreeNode], right: Option[RangeTreeNode]) {

  /**
   * Creates tree structure based on given list of intervals. Each parent note is sum of childes intervals.
   *
   * @param si - list of intervals
   */
  def this(si: Array[SortedInterval[Int]]) = this(
    SortedInterval[Int](si.head.start, si.last.end),
    if (si.length > 1) Some(new RangeTreeNode(si.slice(0, si.length / 2))) else None,
    if (si.length > 1) Some(new RangeTreeNode(si.slice(si.length / 2, si.length))) else None)

  /**
   * Retruns list of partitions to which given interval should belong.
   * One interval can belong to zero or more ranges.
   * @param si - interval
   * @return
   */
  def getPartitions(si: SortedInterval[Int]): Seq[SortedInterval[Int]] = {
    if (si.intersects(this.range)) {
      if (this.left.isEmpty)
        range :: Nil
      else
        this.left.get.getPartitions(si) ++ this.right.get.getPartitions(si)
    }
    else {
      Nil
    }
  }
}
