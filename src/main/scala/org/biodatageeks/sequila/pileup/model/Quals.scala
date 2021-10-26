package org.biodatageeks.sequila.pileup.model

import org.apache.commons.lang3.mutable.MutableInt
import org.apache.spark.broadcast.Broadcast
import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable

object Quals {

  type MultiLociQuals = mutable.HashMap[(Int, Char, Byte), MutableInt]
  val MultiLociQuals = mutable.HashMap[(Int, Char, Byte), MutableInt] _

  implicit class MultiLociQualsExtension(val map: Quals.MultiLociQuals) {
    def ++(that: Quals.MultiLociQuals): Quals.MultiLociQuals = (map ++ that)

    @inline
    def updateQuals(position: Int, base: Char, quality: Byte): Unit = {

      val counter = map.get(position, base, quality)
      if (counter.isEmpty)
        map.put((position, base, quality), new MutableInt(1))
      else
        counter.get.increment()
    }

  }
}
