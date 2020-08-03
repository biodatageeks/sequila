package org.biodatageeks.sequila.pileup.broadcast

import org.biodatageeks.sequila.pileup.broadcast.Correction.PartitionCorrections
import org.biodatageeks.sequila.pileup.broadcast.Shrink.PartitionShrinks
import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.pileup.model.QualityCache
import org.biodatageeks.sequila.utils.FastMath
import org.biodatageeks.sequila.pileup.model.Quals._

import scala.collection.mutable


case class Correction(
                       events: Option[Array[Short]],
                       alts: Option[MultiLociAlts],
                       quals: Option[MultiLociQuals],
                       cumulativeSum: Short,
                       qualityCache:QualityCache
                     )


object Correction {
  type PartitionCorrections = mutable.HashMap[(String,Int), Correction]
  val PartitionCorrections = mutable.HashMap[(String,Int), Correction] _

  implicit class PartitionCorrectionsExtension(val map: PartitionCorrections) {

    def getAlts(contig:String, pos:Int): MultiLociAlts ={
      map.get((contig, pos)) match {
        case Some(correction) =>
          correction.alts match {
            case Some(altsMap) => altsMap
            case None => new MultiLociAlts()
          }
        case None => new MultiLociAlts()
      }
    }

    def getQuals(contig:String, pos:Int): MultiLociQuals ={
      map.get((contig, pos)) match {
        case Some(correction) =>
          correction.quals match {
            case Some(qualsMap) => qualsMap
            case None => new MultiLociQuals()
          }
        case None => new MultiLociQuals()
      }
    }

    def updateWithOverlap(overlap:Tail, range:Range, overlapLen: Int, cumSum: Int): Unit = {

      val arrEvents= Array.fill[Short](math.max(0, overlap.startPoint - range.minPos))(0) ++ overlap.events.takeRight(overlapLen)


      map.get((range.contig, range.minPos))  match {
        case Some(correction) => {
          val newArrEvents = correction.events.get.zipAll(arrEvents, 0.toShort, 0.toShort).map { case (x, y) => (x + y).toShort }
          val newAlts = (correction.alts.get ++ overlap.alts).asInstanceOf[MultiLociAlts]
          val newQuals = (correction.quals.get ++ overlap.quals).asInstanceOf[MultiLociQuals]
          val newCumSum = (correction.cumulativeSum - FastMath.sumShort(overlap.events.takeRight(overlapLen)) ).toShort
          val newCache = correction.qualityCache ++ overlap.cache

          val newCorrection = Correction(Some(newArrEvents), Some(newAlts), Some(newQuals), newCumSum, newCache)

          val coordinates = if (overlap.minPos < range.minPos)
            (range.contig, range.minPos)
          else
            (range.contig, overlap.minPos)

          map.update(coordinates, newCorrection)

        }
        case _ => {
          val newCumSum=(cumSum - FastMath.sumShort(overlap.events.takeRight(overlapLen))).toShort
          val newCorrection = Correction(Some(arrEvents), Some(overlap.alts), Some(overlap.quals),newCumSum, overlap.cache)
          map += (range.contig, range.minPos) -> newCorrection
        }
      }
    }
  }
}

case class FullCorrections (corrections: PartitionCorrections, shrinks:PartitionShrinks)
