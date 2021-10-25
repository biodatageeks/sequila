package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
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

    def allocateArrays (conf: Conf):Unit = {
      val arrSize = conf.qualityArrayLength
      arr('A' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      arr('C' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      arr('T' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      arr('G' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      arr('N' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      arr('a' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      arr('c' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      arr('t' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      arr('g' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      arr('n' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
      //arr(QualityConstants.REF_SYMBOL - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
    }

  }

  implicit class MultiLociQualsExtension(val map: Quals.MultiLociQuals) {
    def ++(that: Quals.MultiLociQuals): Quals.MultiLociQuals = (map ++ that)

    @inline
    def updateQuals(position: Int, base: Char, quality: Byte, conf: Conf): Unit = {
      map(position).addQualityForBase(base, quality, conf)
    }

    def extendAllocation(start: Int, end: Int, oldMax:Int, conf: Conf): Unit = {
      val allocStartPos = if (start > oldMax) start else oldMax + 1
      for (i <- allocStartPos to end) {
        val singleLocusQualMap = new SingleLocusQuals(QualityConstants.OUTER_QUAL_SIZE)
        singleLocusQualMap.allocateArrays(conf)
        map(i) = singleLocusQualMap
      }
    }

  }
}
