package org.biodatageeks.sequila.rangejoins.methods.transformations

import org.biodatageeks.formats.Interval


object RangeMethods {

  /**
    *
    * @param start
    * @param end
    * @param shift
    * @return
    */
  def shift(start: Int, end: Int, shift: Int) :Interval= {

    Interval (start+shift, end+shift)
  }

  /**
    *
    * @param start
    * @param end
    * @param shift
    * @param fix
    * @return
    */
  def resize(start:Int, end: Int, shift: Int, fix:String ): Interval = fix.toLowerCase match {
    case "start" => Interval (start, end + shift)
    case "end" => Interval (start - shift, end)
    case _ => {
      val  width = end - start
      val center = start + width/2
      Interval (center - (width/2 +(if (shift % 2 == 0) shift/2 else shift/2 + 1)) , center + (width/2 + (if (shift % 2 == 0) shift/2 else shift/2) ) )
    }
  }

  /**
    *
    * @param start1
    * @param end1
    * @param start2
    * @param end2
    * @return
    */
  def calcOverlap(start1:Int, end1: Int, start2:Int, end2: Int ) = (math.min(end1,end2) - math.max(start1,start2) + 1)

  /**
    *
    * @param start
    * @param end
    * @param flankWidth
    * @param startFlank
    * @param both
    * @return
    */
  def flank(start: Int, end: Int, flankWidth: Int, startFlank: Boolean, both: Boolean): Interval = {
    if (both) {
      val width = Math.abs(flankWidth)
      val newStart = if (startFlank) (start - width) else (end - width + 1)
      Interval (newStart, newStart + 2 * width - 1)
    } else {
      val newStart = (startFlank, flankWidth >= 0) match {
        case (true, true) => start - flankWidth
        case (true, false) => start
        case (false, true) => end + 1
        case (false, false) => end + flankWidth + 1
      }
      val width = Math.abs(flankWidth)
      Interval (newStart, newStart + width - 1)
    }
  }

  /**
    *
    * @param start
    * @param end
    * @param upstream
    * @param downstream
    * @return
    */
  def promoters(start:Int, end: Int, upstream: Int, downstream: Int): Interval = {
    if (upstream >=0 && downstream >= 0) {
     Interval(start - upstream, start + downstream - 1)
    }
    else {
      throw new Exception("Upstream and downstream must be >= 0")
    }
  }

  /**
    *
    * @param start
    * @param end
    * @param boundStart
    * @param boundEnd
    * @return
    */
  def reflect(start:Int, end: Int, boundStart: Int, boundEnd: Int): Interval = {
    val newStart = (2 * boundStart + boundEnd-boundStart) - end
    Interval(newStart, newStart+end-start)
  }


}
