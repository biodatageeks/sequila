package org.biodatageeks.sequila.pileup.udfs

import scala.collection.mutable

object QualityFunctions {

  def qualsToMap(map: Map[Byte, mutable.WrappedArray[Short]]): Map[Byte, mutable.HashMap[String, Short]] = {

    if (map == null)
      null
    else
      map.map({ case (k, v) => {
        val nestedMap = new mutable.HashMap[String, Short]()
        for (i <- v.indices)
          if (v(i) != 0)
            nestedMap += (i + 33).toChar.toString -> v(i)
        k -> nestedMap
      }
      })
  }
  def qualsToCharMap(map: Map[Byte, mutable.WrappedArray[Short]]): Map[String, mutable.HashMap[String, Short]] = {

    if (map == null)
      null
    else
      map.map({ case (k, v) => {
        val nestedMap = new mutable.HashMap[String, Short]()
        for (i <- v.indices)
          if (v(i) != 0)
            nestedMap += (i + 33).toChar.toString -> v(i)
        k.toChar.toString -> nestedMap
      }
      })
  }

  def qualsToCoverage (map: Map[Byte, mutable.WrappedArray[Short]], cov:Short): Short = {
    if (map == null)
       cov
    else
      map.map({case (k,v) => v.sum}).sum
  }



}
