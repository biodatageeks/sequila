package org.biodatageeks.sequila.pileup.broadcast

import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable.ArrayBuffer

case class Range(contig: String, minPos: Int, maxPos: Int) {

  def findOverlaps(tails: ArrayBuffer[Tail]):ArrayBuffer[Tail] = {
    tails
      .filter(t => (t.contig == contig && t.startPoint + t.events.length > minPos) && t.minPos < minPos)
      .sortBy(r => (r.contig, r.minPos))
  }

  def precedingCumulativeSum(tails: ArrayBuffer[Tail]): Short = {
    val tailsFiltered = tails
      .filter(t => t.contig == contig && t.minPos < minPos)
    val cumSum = tailsFiltered.map(_.cumSum)
    val cumsSumArr = cumSum.toArray
    FastMath.sumShort(cumsSumArr)
  }

  def calculateOverlapLength(o: Tail,  it: Int, ranges: ArrayBuffer[Range]): Int = {
    val length = if ((o.startPoint + o.events.length) > maxPos && ((ranges.length - 1 == it) || ranges(it + 1).contig != contig))
      o.startPoint + o.events.length - minPos + 1
    else if ((o.startPoint + o.events.length) > maxPos)
      maxPos - minPos
    //if it's the last part in contig or the last at all
    else
      o.startPoint + o.events.length - minPos + 1
    length
  }

}