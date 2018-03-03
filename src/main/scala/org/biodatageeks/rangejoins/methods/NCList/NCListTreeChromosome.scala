package org.biodatageeks.rangejoins.methods.NCList

import org.biodatageeks.rangejoins.NCList.{Interval, NCListTree}

import scala.collection.mutable.{ArrayBuffer, ArrayStack}
import scala.util.control.Breaks._

class NCListTreeChromosome[T](allRegions: Array[((String,Interval[Int]), T)]) extends Serializable {

  val nclistTreeHashMap:Map[String,NCListTree[T]] = allRegions.groupBy(_._1._1).map(x => (x._1,new NCListTree[T](x._2.map(y => (y._1._2,y._2)))))

  def getAllOverlappings(r: (String,Interval[Int])) = nclistTreeHashMap.get(r._1) match {
    case Some(t) => t.getAllOverlappings(r._2)
    case _ => Nil
  }

}
