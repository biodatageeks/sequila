package org.biodatageeks.sequila.pileup.udfs

import scala.collection.mutable

object AltFunctions {
  def byteToString(map: Map[Byte, Short]): Map[String, Short] = {
    if (map == null)
      return null

    map.map({ case (k, v) =>
      k.toChar.toString -> v
    })
  }

  def altMapToString (map: Map[Any, Any]) : String = {
    if (map == null)
      return null
    map.map({
      case (k, v) => k.toString -> v
    }).toSeq.sortBy(_._1).mkString
  }

}
