package org.biodatageeks.sequila.pileup.model

import scala.collection.mutable

object Alts {
  type SingleLocusAlts = mutable.HashMap[Byte,Short]
  val SingleLocusAlts = mutable.HashMap[Byte,Short] _


  type MultiLociAlts= mutable.IntMap [SingleLocusAlts]
  val MultiLociAlts = mutable.IntMap [SingleLocusAlts] _

  implicit class SingleLocusAltsExtension(val map: Alts.SingleLocusAlts) {
    def derivedAltsNumber:Int = {
      var sum = 0
      for (i <- map.keySet)
        sum += map(i)
      sum
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
  }

}



