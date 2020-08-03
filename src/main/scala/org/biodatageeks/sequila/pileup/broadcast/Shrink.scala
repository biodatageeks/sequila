package org.biodatageeks.sequila.pileup.broadcast

import scala.collection.mutable

case class Shrink (index:Int)

object Shrink {
  type PartitionShrinks = mutable.HashMap[(String,Int), Shrink]
  val PartitionShrinks = mutable.HashMap[(String,Int), Shrink] _

  implicit class PartitionShrinksExtension(val map: PartitionShrinks) {
    def updateWithOverlap(overlap: Tail, range: Range): Unit = {
      val coordinates = (overlap.contig, overlap.minPos)
      map.get(coordinates) match {
        case Some(oldShrink) => map.update(coordinates, Shrink(math.min(oldShrink.index, range.minPos - overlap.minPos + 1)))
        case _ => map += coordinates -> Shrink(range.minPos - overlap.minPos + 1)

      }
    }
  }
}