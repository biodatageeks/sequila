package org.biodatageeks.rangejoins.methods.transformations

object RangeMethods {

  /**
    *
    * @param value
    * @param shift
    * @return
    */
  def shift(value: Int, shift: Int) :Int= {

    value + shift
  }

  /**
    *
    * @param start
    * @param end
    * @param shift
    * @param fix
    * @return
    */
  def resize(start:Int, end: Int, shift: Int, fix:String ): (Int,Int) = fix.toLowerCase match {
    case "start" => (start, end + shift)
    case "end" => (start - shift, end)
    case _ => {
      val  width = end - start
      val center = start + width/2
      (center - (width/2 +(if (shift % 2 == 0) shift/2 else shift/2 + 1)) , center + (width/2 + (if (shift % 2 == 0) shift/2 else shift/2) ) )
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


}
