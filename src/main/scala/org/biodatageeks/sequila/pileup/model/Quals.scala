package org.biodatageeks.sequila.pileup.model

import org.apache.spark.broadcast.Broadcast
import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable

object Quals {
  type SingleLocusQuals =  mutable.ListBuffer[(Char, Byte)]
  val SingleLocusQuals = mutable.ListBuffer[(Char, Byte)] _

  type MultiLociQuals = mutable.IntMap[Quals.SingleLocusQuals]
  val MultiLociQuals = mutable.IntMap[Quals.SingleLocusQuals] _

  implicit class SingleLocusQualsExtension(val arr: Quals.SingleLocusQuals) {
    //def derivedCoverage: Short = arr.flatMap(x => if (x != null) x.toList else List.empty).sum

    //def totalEntries: Long = arr.flatMap(_.toList).count(_ != 0)

//    def merge(arrOther: SingleLocusQuals): SingleLocusQuals = {
//      arr.zip(arrOther).map { case (x, y) =>
//        if (x == null) y
//        else if (y == null) x
//        else x.zipAll(y, 0.toShort, 0.toShort).map(a => (a._1 + a._2).toShort)
//      }}


    def addQualityForBase(base: Char, quality: Byte, conf: Conf): Unit = {
      arr.append((base,quality))
    }

//    def allocateArrays (conf: Conf):Unit = {
//      val arrSize = conf.qualityArrayLength
//      arr('A' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      arr('C' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      arr('T' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      arr('G' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      arr('N' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      arr('a' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      arr('c' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      arr('t' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      arr('g' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      arr('n' - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//      //arr(QualityConstants.REF_SYMBOL - QualityConstants.QUAL_INDEX_SHIFT) = new Array[Short](arrSize)
//    }

  }

  implicit class MultiLociQualsExtension(val map: Quals.MultiLociQuals) {
    def ++(that: Quals.MultiLociQuals): Quals.MultiLociQuals = (map ++ that)

    @inline
    def updateQuals(position: Int, base: Char, quality: Byte, conf: Conf): Unit = {
      val quals = map.get(position)
      if (quals.isDefined)
        quals.get.addQualityForBase(base, quality, conf)
      else {
        val singleLocusQualMap = new SingleLocusQuals()
        singleLocusQualMap.addQualityForBase(base, quality, conf)
        map.update(position, singleLocusQualMap)
      }
    }


//    def getTotalEntries: Long = {
//      map.map { case (k, v) => k -> map(k).totalEntries }.foldLeft(0L)(_ + _._2)
//    }

  }
}
