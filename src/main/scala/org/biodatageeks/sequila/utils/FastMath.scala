package org.biodatageeks.sequila.utils

import org.biodatageeks.sequila.pileup.model.{MultiLociAlts, SingleLocusAlts}

import scala.collection.mutable

object FastMath {

  def sumShort(a: Array[Short]) = {
    var i = 0
    var cumSum = 0
    while(i < a.length){
      cumSum += a(i)
      i+=1
    }
    cumSum.toShort
  }

  /**
    * finds index of the last non zero element of array
    * @param array array of Short elements
    * @return index
    */

  def findMaxIndex(array: Array[Short]): Int = {
    var i = array.length - 1

    while (i > 0) {
      if (array(i) != 0)
        return i
      i -= 1
    }
    -1
  }

  def mergeMaps(map1: SingleLocusAlts, map2: SingleLocusAlts): SingleLocusAlts = {
    if (map1 == null)
      return map2
    if (map2 == null)
      return map1
    if(map1.keySet.intersect(map2.keySet).isEmpty) return map1 ++ map2
    val keyset = map1.keySet++map2.keySet
    val mergedMap = new SingleLocusAlts()
    for (k <- keyset)
      mergedMap(k) = (map1.getOrElse(k, 0.toShort) + map2.getOrElse(k, 0.toShort)).toShort
    mergedMap
  }

  def mergeNestedMaps(map1: MultiLociAlts, map2: MultiLociAlts): MultiLociAlts = {
    if (map1 == null || map1.isEmpty)
      return map2
    if (map2 == null || map2.isEmpty)
      return map1
    if(map1.keySet.intersect(map2.keySet).isEmpty) return map1 ++ map2
    val keyset = map1.keySet++map2.keySet
    var mergedAltsMap = new MultiLociAlts()
    for (k <- keyset)
      mergedAltsMap += k -> mergeMaps(map1.getOrElse(k,null), map2.getOrElse(k,null)) // to refactor
    mergedAltsMap
  }


}
