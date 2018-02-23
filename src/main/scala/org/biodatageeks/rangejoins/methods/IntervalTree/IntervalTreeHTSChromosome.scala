package org.biodatageeks.rangejoins.methods.IntervalTree

import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.rangejoins.IntervalTree.{Interval, IntervalWithRow}

class IntervalTreeHTSChromosome[T](allRegions: List[(String,Interval[Int],T)]) extends Serializable {

  val intervalTreeHashMap:Map[String,IntervalTreeHTS[T]] = allRegions
    .groupBy(_._1)
    .map(x => {
      val it = new IntervalTreeHTS[T]()
      x._2.map(y => it.put(y._2.start,y._2.end,y._3))
      (x._1, it)
  })

  def getIntervalTreeByChromosome(chr:String): IntervalTreeHTS[T] = intervalTreeHashMap(chr)

}
