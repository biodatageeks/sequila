package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable

object Alts {
  type SingleLocusAlts = mutable.HashMap[Byte,Short]
  val SingleLocusAlts = mutable.HashMap[Byte,Short] _


  type MultiLociAlts= mutable.LongMap [SingleLocusAlts]
  val MultiLociAlts = mutable.LongMap [SingleLocusAlts] _

  implicit class SingleLocusAltsExtension(val map: Alts.SingleLocusAlts) {
    def derivedAltsNumber:Short = map.foldLeft(0)(_+_._2).toShort

    def merge(mapOther: SingleLocusAlts): SingleLocusAlts = {
      val fastMerge = FastMath.merge(map, mapOther)
      if (fastMerge.isDefined)
        return FastMath.merge(map, mapOther).get.asInstanceOf[SingleLocusAlts]

      val keys = map.keySet ++ mapOther.keySet
      val mergedMap = new SingleLocusAlts()
      for (k <- keys)
        mergedMap(k) = (map.getOrElse(k, 0.toShort) + mapOther.getOrElse(k, 0.toShort)).toShort
      mergedMap
    }
  }

  implicit class MultiLociAltsExtension (val map: Alts.MultiLociAlts) {
    def ++ (that: Alts.MultiLociAlts): Alts.MultiLociAlts = (map ++ that).asInstanceOf[Alts.MultiLociAlts]

    def updateAlts(pos: Int, alt: Char): Unit = {
      val position = pos // naturally indexed
      val altByte = alt.toByte

      val altMap = map.getOrElse(position, new Alts.SingleLocusAlts())
      altMap(altByte) = (altMap.getOrElse(altByte, 0.toShort) + 1).toShort
      map.update(position, altMap)
    }

    def merge(mapOther: MultiLociAlts): MultiLociAlts = {
      val fastMerge = FastMath.merge(map, mapOther)
      if (fastMerge.isDefined)
        return FastMath.merge(map, mapOther).get.asInstanceOf[MultiLociAlts]

      val keyset = map.keySet ++ mapOther.keySet
      var mergedAltsMap = new MultiLociAlts()
      for (k <- keyset)
        mergedAltsMap += k -> map.getOrElse(k, new SingleLocusAlts()).merge(mapOther.getOrElse(k, new SingleLocusAlts()))
      mergedAltsMap
    }

    def getPositionsForRange(start:Int, end: Int):Array[Long] =
      FastMath.getSubArrayForRange(map.keySet.toArray[Long], start, end)


  }




}



