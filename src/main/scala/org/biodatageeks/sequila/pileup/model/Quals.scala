package org.biodatageeks.sequila.pileup.model

import org.apache.spark.broadcast.Broadcast
import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable

object Quals {
  type SingleLocusQuals = Array[Array[Short]]
  val SingleLocusQuals = Array[Array[Short]] _

  type MultiLociQuals = mutable.IntMap[Quals.SingleLocusQuals]
  val MultiLociQuals = mutable.IntMap[Quals.SingleLocusQuals] _

  implicit class SingleLocusQualsExtension(val arr: Quals.SingleLocusQuals) {
    def derivedCoverage: Short = arr.flatMap(x => if (x != null) x.toList else List.empty).sum

    def totalEntries: Long = arr.flatMap(_.toList).count(_ != 0)

    def merge(arrOther: SingleLocusQuals): SingleLocusQuals = {
      arr.zip(arrOther).map { case (x, y) =>
        if (x == null) y
        else if (y == null) x
        else x.zipAll(y, 0.toShort, 0.toShort).map(a => (a._1 + a._2).toShort)
      }}


    def addQualityForBase(base: Char, quality: Byte, conf: Conf): Unit = {
      val qualityIndex = if (conf.isBinningEnabled) (quality / conf.binSize).toShort else quality
      val altArrIndex = base - QualityConstants.QUAL_INDEX_SHIFT
      arr(altArrIndex)(qualityIndex) = (arr(altArrIndex)(qualityIndex) + 1).toShort
    }

  }

  implicit class MultiLociQualsExtension(val map: Quals.MultiLociQuals) {
    def ++(that: Quals.MultiLociQuals): Quals.MultiLociQuals = (map ++ that)

    @inline
    def updateQuals(position: Int, base: Char, quality: Byte, conf: Conf): Unit = {

      val baseIdx = mapBaseToIdx(base)

      map.get(position) match {
        case None => {
          val arr =  new Array[Array[Short]](10)
          arr(baseIdx) = new Array[Short](41)
          arr(baseIdx)(quality) = 1
          map.put(position, arr)
        }
        case Some(posPointer) => {

          if(posPointer(baseIdx) == null) {
            posPointer(baseIdx) = new Array[Short](41)
            posPointer(baseIdx)(quality) = 1
          }
          else posPointer(baseIdx)(quality) = (posPointer(baseIdx)(quality) + 1).toShort


        }
      }
//      if (map.contains(position)) {
//        map(position).addQualityForBase(base, quality, conf)
//      }
//      else {
//        val singleLocusQualMap = new SingleLocusQuals(QualityConstants.OUTER_QUAL_SIZE)
//        singleLocusQualMap.addQualityForBase(base, quality, conf)
//        map.update(position, singleLocusQualMap)
//      }
    }

    def getTotalEntries: Long = {
      map.map { case (k, v) => k -> map(k).totalEntries }.foldLeft(0L)(_ + _._2)
    }

  }

  def mapBaseToIdx(base: Char): Int = {
    base match {
      case 'A' => 0
      case 'C' => 1
      case 'T' => 2
      case 'G' => 3
      case 'N' => 4
      case 'a' => 5
      case 'c' => 6
      case 't' => 7
      case 'g' => 8
      case 'n' => 9
    }
  }

  def mapIdxToBase(ind: Int): Char = {
    ind match {
      case 0 => 'A'
      case 1 => 'C'
      case 2 => 'T'
      case 3 => 'G'
      case 4 => 'N'
      case 5 => 'a'
      case 6 => 'c'
      case 7 => 't'
      case 8 => 'g'
      case 9 => 'n'
    }
  }
}
