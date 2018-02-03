package org.biodatageeks.rangejoins.NCList

import scala.collection.mutable

object NCListBuilder {
  def build[T](list: List[(Interval[Int], T)]): NCList = {

    var topNCList = NCList(Array.empty[NCList], 0, Array.empty[Int])
    var landingNCList = NCList(Array.empty[NCList], 0, Array.empty[Int])

    var listWithIndices = list.zipWithIndex.map{case (k,v) => (v,k)}
    var sortedIndices = listWithIndices.sortWith((x, y) => x._2._1.end > y._2._1.end)
      .sortWith((x, y) => x._2._1.start < y._2._1.start)
      .map(x => x._1)

    var stack = mutable.Stack[NCListBuildingStack]()


    for( i <- 0 until sortedIndices.length) {
      var rgid = sortedIndices(i)
      var currentEnd = listWithIndices(rgid)._2._1.end
      while(!stack.isEmpty && listWithIndices(stack.top.rgid)._2._1.end < currentEnd)
        stack.pop

      landingNCList = if (stack.isEmpty) topNCList else stack.top.ncList

      var stackElt = appendNCListElt(landingNCList, rgid)
      stack.push(stackElt)
    }

    topNCList
  }

   def appendNCListElt(landingNCList: NCList, rgid: Int): NCListBuildingStack = {
     var nChildren = landingNCList.nChildren
     landingNCList.childrenBuf :+= NCList(Array.empty[NCList], 0, Array.empty[Int])
     var childrenNCList = landingNCList.childrenBuf.last
     var stackElt = NCListBuildingStack(childrenNCList,rgid)
     landingNCList.rgidBuf :+= rgid
     landingNCList.nChildren = landingNCList.nChildren+1

     stackElt
   }
}
