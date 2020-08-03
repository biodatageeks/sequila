package org.biodatageeks.sequila.utils

import scala.collection.mutable

object FastMath {
  def getSubArrayForRange(input: Array[Long], start: Int, end: Int):Array[Long] = {
    val sortedArray = input.sorted
    val output = new Array[Long](input.size)
    var currPosition = input.size-1
    var outputPos = 0

    while(currPosition >=0){
      val item = sortedArray(currPosition)
      if (item >= start && item <= end) {
        output(outputPos) = item
        outputPos += 1
      }
      currPosition -=1

      if (item < start)
        return output.take(outputPos)
    }
    output.take(outputPos)

  }

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

  def merge [A,B](map1: mutable.Map[A,B], map2:mutable.Map[A,B] ): Option[mutable.Map[A,B]] ={
    if (map1 == null || map1.isEmpty)
      return Option(map2)
    if (map2 == null || map2.isEmpty)
      return Option(map1)
    if(map1.keySet.intersect(map2.keySet).isEmpty)
      return Option(map1 ++ map2)
    None
  }


}
