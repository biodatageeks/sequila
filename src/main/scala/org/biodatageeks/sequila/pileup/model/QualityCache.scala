package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.pileup.conf.QualityConstants
import scala.collection.mutable.ArrayBuffer

class QualityCache(size: Int) extends Serializable {
  var cache = new Array[ReadQualSummary](QualityConstants.CACHE_EXPANDER*size)
  val rollingIndexStart = size
  var currentIndex = 0
  var isFull = false

  def this (qualityArray:Array[ReadQualSummary] ) {
    this(qualityArray.size/2)
    this.cache=qualityArray
  }

  def copy:QualityCache = {
    val newCache = new QualityCache(size)
    cache.copyToArray(newCache.cache)
    newCache
  }
  def length: Int = cache.length
  def apply(index: Int):ReadQualSummary = cache(index)

  def ++ (that:QualityCache):QualityCache = {
    val mergedArray = new Array[ReadQualSummary](length + that.length)
    System.arraycopy(this.cache, 0, mergedArray, 0, length)
    System.arraycopy(that.cache, 0, mergedArray, length, that.length)
    new QualityCache(mergedArray)
  }

  def addOrReplace(readSummary: ReadQualSummary):Unit = {
    cache(currentIndex) = null
    cache(currentIndex) = readSummary
    if (currentIndex + 1 >= length) {
      currentIndex = rollingIndexStart
      isFull = true
    }
    else currentIndex = currentIndex + 1
  }

  def initSearchIndex:Int ={
    if(currentIndex==0) 0
    else if (!isFull) currentIndex -1
    else if (isFull && currentIndex == rollingIndexStart) cache.length-1
    else currentIndex-1
  }

  def getReadsOverlappingPosition(position: Int): Array[ReadQualSummary] = {
    var currPos =  if(currentIndex==0) 0
    else if (!isFull) currentIndex -1
    else if (isFull && currentIndex == rollingIndexStart) cache.length-1
    else currentIndex-1

    val rs = cache(currPos)
    if(rs == null || rs.start > position)
      return Array.empty[ReadQualSummary]
    else {
      var it = 0
      val maxIterations = (cache.length/2)-1
      val buffer = new ArrayBuffer[ReadQualSummary]()
      while (it <= maxIterations) {
        val rs = cache(currPos)
        if (rs == null || rs.start > position)
          return buffer.toArray
        else if (!rs.hasDeletionOnPosition(position) && rs.start <= position && rs.end >= position) // overlapsPosition
          buffer.append(rs)

        if (isFull) {
          if (currPos == rollingIndexStart) currPos = cache.length - 1
          else currPos -= 1
        } else if (!isFull) {
          if (currPos == 0)
            return buffer.toArray
          else
            currPos -= 1
        }
        it += 1
      }
      buffer.toArray
    }
  }

  def getReadsOverlappingPositionFullCache(position: Int): Array[ReadQualSummary] = {
    val buffer = new ArrayBuffer[ReadQualSummary]()
        for (rs <- cache) {
          if (rs == null )
            return buffer.toArray
          else if (!rs.hasDeletionOnPosition(position) && rs.start <= position && rs.end >= position) // overlapsPosition
            buffer.append(rs)
        }
    buffer.toArray
  }

}

