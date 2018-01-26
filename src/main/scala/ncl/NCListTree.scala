package ncl

import scala.collection.mutable
import scala.util.control.Breaks._

class NCListTree[T](allRegions: List[(Interval[Long], T)]) extends Serializable {

  val ncList = NCListBuilder.build(allRegions)

  def getAllOverlappings(processedInterval: Interval[Long]) = allOverlappingRegions(processedInterval, ncList, allRegions)

  private def allOverlappingRegions(processedInterval: Interval[Long], topNcList: NCList, intervalList: List[(Interval[Long],T)]): List[(Interval[Long], T)] = {
    var backpack = Backpack(intervalList, processedInterval)
    var resultList = List[(Interval[Long], T)]()
    var walkingStack = mutable.Stack[NCListWalkingStack]()
    walkingStack.clear()

    var n = findLandingChild(topNcList, backpack)
    if (n < 0)
      return Nil

    var ncList = moveToChild(topNcList, n, walkingStack)
    while (ncList != null) {
      var stackElt = peekNCListWalkingStackElt(walkingStack)
      var rgid = stackElt.parentNcList.rgidBuf(stackElt.n)
      breakable {
        if (backpack.intervalList(rgid)._1.start > backpack.processedInterval.end) {
          /* Skip all further siblings of 'nclist'. */
          ncList = moveToRightUncle(walkingStack)
          break //continue
        }

        resultList :+= intervalList(rgid)
        n = findLandingChild(ncList, backpack)
        /* Skip first 'n' or all children of 'nclist'. */
        ncList = if (n >= 0) moveToChild(ncList, n, walkingStack) else moveToRightSiblingOrUncle(ncList, walkingStack)
      }
    }
    resultList
  }

  private def findLandingChild(ncList: NCList, backpack: Backpack[T]): Int = {
    var nChildren = ncList.nChildren
    if (nChildren == 0)
      return -1;

    var n = intBsearch(ncList.rgidBuf, nChildren, backpack.intervalList.toArray.map(_._1.end), backpack.processedInterval.start)

    if (n >= nChildren)
      return -1;

    return n
  }

  private def intBsearch(subset: Array[Int], subsetLen: Int, base: Array[Long], min: Long): Int = {
    /* Check first element. */
    var n1 = 0
    var b = base(subset(n1))
    if (b >= min)
      return n1

    /* Check last element. */
    var n2 = subsetLen - 1
    b = base(subset(n2))
    if (b < min)
      return subsetLen
    if (b == min)
      return n2

    /* Binary search.*/
    var n = (n1 + n2) / 2
    while (n != n1) {
      b = base(subset(n))
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

  private def moveToChild(parentNcList: NCList, n: Int, walkingStack: mutable.Stack[NCListWalkingStack]): NCList = {
    walkingStack.push(NCListWalkingStack(parentNcList, n))
    parentNcList.childrenBuf(n)
  }

  private def peekNCListWalkingStackElt(walkingStack: mutable.Stack[NCListWalkingStack]): NCListWalkingStack = {
    walkingStack.top
  }

  private def moveToRightUncle(walkingStack: mutable.Stack[NCListWalkingStack]): NCList = {
    var parentNcList = walkingStack.pop().parentNcList
    if (walkingStack.isEmpty)
      return null
    return moveToRightSiblingOrUncle(parentNcList, walkingStack)
  }

  private def moveToRightSiblingOrUncle(ncList: NCList, walkingStack: mutable.Stack[NCListWalkingStack]): NCList = {
    var ncListLocal = ncList
    var stackEltPlusPlus: NCListWalkingStack = null

    do {
      var stackElt = walkingStack.pop()
      if ((stackElt.n+1) < stackElt.parentNcList.nChildren) {
        walkingStack.push(NCListWalkingStack(stackElt.parentNcList,stackElt.n+1))
        ncListLocal = stackElt.parentNcList.childrenBuf(stackElt.n+1)
        return ncListLocal
      } else {
        walkingStack.push(NCListWalkingStack(stackElt.parentNcList,stackElt.n+1))
        ncListLocal = stackElt.parentNcList
        walkingStack.pop()
      }
    } while (!walkingStack.isEmpty)
    return null
  }

}
