package org.biodatageeks.sequila.pileup.model

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

    def trim: SingleLocusQuals = {
      arr.map({ array =>
        if (array != null) {
          val maxIndex = array.last
          if (maxIndex < QualityConstants.MAX_QUAL_IND)
            array.take(array(Conf.qualityArrayLength - 1) + 1)
          else
            array
        }
        else null
      })
    }

    def addQualityForAlt(alt: Char, quality: Byte, updateMax: Boolean): Unit = {
      val qualityIndex = if (Conf.isBinningEnabled) (quality / Conf.binSize).toShort else quality
      val arrSize = Conf.qualityArrayLength
      val altArrIndex = alt - QualityConstants.QUAL_INDEX_SHIFT

      if (arr(altArrIndex) == null) {
        val array = new Array[Short](arrSize)
        array(qualityIndex) = 1.toShort // no need for incrementing. first and last time here.
        array(arrSize - 1) = qualityIndex
        arr(altArrIndex) = array
        return
      }

      if (updateMax) {
        arr(altArrIndex)(qualityIndex) = (arr(altArrIndex)(qualityIndex) + 1).toShort
        if (qualityIndex > arr(altArrIndex).last)
          arr(altArrIndex)(arrSize - 1) = qualityIndex
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

    def trim: MultiLociQuals = map.map({ case (k, v) => k -> v.trim })

    @inline
    def updateQuals(position: Int, alt: Char, quality: Byte, firstUpdate: Boolean = false, updateMax: Boolean = false): Unit = {
      if (!firstUpdate || map.contains(position)) {
        map(position).addQualityForAlt(alt, quality, updateMax)
      }
      else {
        val singleLocusQualMap = new SingleLocusQuals(QualityConstants.OUTER_QUAL_SIZE)
        singleLocusQualMap.addQualityForAlt(alt, quality, updateMax)
        map.update(position, singleLocusQualMap)
      }
    }

    def merge(mapOther: MultiLociQuals): MultiLociQuals = {
      val fastMerge = FastMath.merge(map, mapOther)
      if (fastMerge.isDefined)
        return fastMerge.get.asInstanceOf[MultiLociQuals]

      var mergedQualsMap = new MultiLociQuals()
      for (k <- map.keySet ++ mapOther.keySet) {
        mergedQualsMap += k -> map.getOrElse(k, new SingleLocusQuals(QualityConstants.OUTER_QUAL_SIZE)).merge(mapOther.getOrElse(k, new SingleLocusQuals(QualityConstants.OUTER_QUAL_SIZE)))
      }
      mergedQualsMap
    }

    def getTotalEntries: Long = {
      map.map { case (k, v) => k -> map(k).totalEntries }.foldLeft(0L)(_ + _._2)
    }

  }
}
