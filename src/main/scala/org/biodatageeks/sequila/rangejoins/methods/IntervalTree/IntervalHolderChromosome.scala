package org.biodatageeks.sequila.rangejoins.methods.IntervalTree

import org.biodatageeks.sequila.rangejoins.IntervalTree.Interval
import org.biodatageeks.sequila.rangejoins.methods.base.BaseIntervalHolder

class IntervalHolderChromosome[T](allRegions: Array[(String,Interval[Int],T)]) extends Serializable {

  val intervalHolderHashMap:Map[String,BaseIntervalHolder[T]] = {

    allRegions
      .groupBy(_._1)
      .map(x => {
        val it = new IntervalTreeHTS[T]()
        x._2.foreach { y =>
          it.put(y._2.start, y._2.end, y._3)
        }
        (x._1, it)
      })
  }

  def getIntervalTreeByChromosome(chr:String): Option[BaseIntervalHolder[T]] = intervalHolderHashMap.get(chr)

}
