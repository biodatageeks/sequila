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

    def addQualityForBase(base: Char, quality: Byte, conf: Broadcast[Conf]): Unit = {
      val qualityIndex = if (conf.value.isBinningEnabled) (quality / conf.value.binSize).toShort else quality
      val arrSize = conf.value.qualityArrayLength
      val altArrIndex = base - QualityConstants.QUAL_INDEX_SHIFT

      if (arr(altArrIndex) == null) {
        val array = new Array[Short](arrSize)
        array(qualityIndex) = 1.toShort // no need for incrementing. first and last time here.
        arr(altArrIndex) = array
        return
      }

      if (qualityIndex >= arr(altArrIndex).length) {
        val array = new Array[Short](arrSize)
        System.arraycopy(arr(altArrIndex), 0, array, 0, arr(altArrIndex).length)
        array(qualityIndex) = 1.toShort
        arr(altArrIndex) = array
        return
      }
      arr(altArrIndex)(qualityIndex) = (arr(altArrIndex)(qualityIndex) + 1).toShort
    }
  }

  implicit class MultiLociQualsExtension(val map: Quals.MultiLociQuals) {
    def ++(that: Quals.MultiLociQuals): Quals.MultiLociQuals = (map ++ that)



    @inline
    def updateQuals(position: Int, base: Char, quality: Byte, conf: Broadcast[Conf]): Unit = {
      if (map.contains(position)) {
        map(position).addQualityForBase(base, quality, conf)
      }
      else {
        val singleLocusQualMap = new SingleLocusQuals(QualityConstants.OUTER_QUAL_SIZE)
        singleLocusQualMap.addQualityForBase(base, quality, conf)
        map.update(position, singleLocusQualMap)
      }
    }


    def getTotalEntries: Long = {
      map.map { case (k, v) => k -> map(k).totalEntries }.foldLeft(0L)(_ + _._2)
    }

  }
}
