package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable

object Alts {
  type SingleLocusAlts = mutable.HashMap[Byte,Short]
  val SingleLocusAlts = mutable.HashMap[Byte,Short] _


  type MultiLociAlts= mutable.IntMap [SingleLocusAlts]
  val MultiLociAlts = mutable.IntMap [SingleLocusAlts] _

  implicit class SingleLocusAltsExtension(val map: Alts.SingleLocusAlts) {
    def derivedAltsNumber:Short = map.foldLeft(0)(_+_._2).toShort

    def merge(mapOther: SingleLocusAlts): SingleLocusAlts = {
      val fastMerge = FastMath.merge(map, mapOther)
      if (fastMerge.isDefined)
        return fastMerge.get.asInstanceOf[SingleLocusAlts]

      val mergedMap = new SingleLocusAlts()
      for (k <- map.keySet ++ mapOther.keySet)
        mergedMap(k) = (map.getOrElse(k, 0.toShort) + mapOther.getOrElse(k, 0.toShort)).toShort
      mergedMap
    }
  }

  implicit class MultiLociAltsExtension (val map: Alts.MultiLociAlts) {
    def ++ (that: Alts.MultiLociAlts): Alts.MultiLociAlts = (map ++ that)

    def updateAlts(position: Int, alt: Char): Unit = {
      val altByte = alt.toByte

      val altMap = map.getOrElse(position, new Alts.SingleLocusAlts())
      altMap(altByte) = (altMap.getOrElse(altByte, 0.toShort) + 1).toShort
      map.update(position, altMap)
    }

    def merge(mapOther: MultiLociAlts): MultiLociAlts = {
      if (FastMath.merge(map, mapOther).isDefined)
        return FastMath.merge(map, mapOther).get.asInstanceOf[MultiLociAlts]

      var mergedAltsMap = new MultiLociAlts()
      for (k <- map.keySet ++ mapOther.keySet)
        mergedAltsMap += k -> map.getOrElse(k, new SingleLocusAlts()).merge(mapOther.getOrElse(k, new SingleLocusAlts()))
      mergedAltsMap
    }
  }

}



