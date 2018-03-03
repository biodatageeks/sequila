package org.biodatageeks.rangejoins.NCList


import scala.collection.mutable.{ArrayBuffer, ArrayStack}
import scala.util.control.Breaks._

class NCListTree[T](allRegions: Array[(Interval[Int], T)]) extends Serializable {

  val ncList = NCListBuilder.build(allRegions)

  def getAllOverlappings(processedInterval: Interval[Int]) = allOverlappingRegions(processedInterval, ncList, allRegions)

  private def allOverlappingRegions(processedInterval: Interval[Int], topNcList: NCList, intervalArray: Array[(Interval[Int],T)]): List[(Interval[Int], T)] = {
    val backpack = Backpack(intervalArray,processedInterval)
    var resultList = List[(Interval[Int], T)]()
    val walkingStack = ArrayStack[NCListWalkingStack]()

    var n = findLandingChild(topNcList, backpack)
    if (n < 0)
      return Nil

    var ncList = moveToChild(topNcList, n, walkingStack)
    while (ncList != null) {
      val stackElt = peekNCListWalkingStackElt(walkingStack)
      val rgid = stackElt.parentNcList.rgidBuf(stackElt.n)
      breakable {
        val candidateInterval = intervalArray(rgid)
        if (candidateInterval._1.start > backpack.processedInterval.end) {
          /* Skip all further siblings of 'nclist'. */
          ncList = moveToRightUncle(walkingStack)
          break //continue
        }

        resultList :+= candidateInterval
        n = findLandingChild(ncList, backpack)
        /* Skip first 'n' or all children of 'nclist'. */
        ncList = if (n >= 0) moveToChild(ncList, n, walkingStack) else moveToRightSiblingOrUncle(ncList, walkingStack)
      }
    }
    resultList
  }

  private def findLandingChild(ncList: NCList, backpack: Backpack[T]): Int = {
    val nChildren = ncList.nChildren
    if (nChildren == 0)
      return -1

    val n = intBsearch(ncList.rgidBuf, nChildren, backpack.intervalArray, backpack.processedInterval.start)

    if (n >= nChildren)
      return -1

    return n
  }

  private def intBsearch(subset: ArrayBuffer[Int], subsetLen: Int, base: Array[(Interval[Int],T)], min: Int): Int = {
    /* Check first element. */
    var n1 = 0
    var b = base(subset(n1))._1.end
    if (b >= min)
      return n1

    /* Check last element. */
    var n2 = subsetLen - 1
    b = base(subset(n2))._1.end
    if (b < min)
      return subsetLen
    if (b == min)
      return n2

    /* Binary search.*/
    var n = (n1 + n2) / 2
    while (n != n1) {
      b = base(subset(n))._1.end
      if (b == min)
        return n
      if (b < min)
        n1 = n
      else
        n2 = n

      n = (n1 + n2) / 2
    }
    return n2
  }

  private def moveToChild(parentNcList: NCList, n: Int, walkingStack: ArrayStack[NCListWalkingStack]): NCList = {
    walkingStack.push(NCListWalkingStack(parentNcList, n))
    parentNcList.childrenBuf(n)
  }

  private def peekNCListWalkingStackElt(walkingStack: ArrayStack[NCListWalkingStack]): NCListWalkingStack = {
    walkingStack.top
  }

  private def moveToRightUncle(walkingStack: ArrayStack[NCListWalkingStack]): NCList = {
    val parentNcList = walkingStack.pop().parentNcList
    if (walkingStack.isEmpty)
      return null
    moveToRightSiblingOrUncle(parentNcList, walkingStack)
  }

  private def moveToRightSiblingOrUncle(ncList: NCList, walkingStack: ArrayStack[NCListWalkingStack]): NCList = {
    var ncListLocal = ncList

    do {
      val stackElt = walkingStack.pop()
      if ((stackElt.n+1) < stackElt.parentNcList.nChildren) {
        walkingStack.push(NCListWalkingStack(stackElt.parentNcList,stackElt.n+1))
        ncListLocal = stackElt.parentNcList.childrenBuf(stackElt.n+1)
        return ncListLocal
      } else {
        walkingStack.push(NCListWalkingStack(stackElt.parentNcList,stackElt.n+1))
        ncListLocal = stackElt.parentNcList
        walkingStack.pop()
      }
    } while (walkingStack.nonEmpty)
    null
  }

}
