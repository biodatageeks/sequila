package org.biodatageeks.sequila.pileup.converters

import scala.collection.mutable.ArrayBuffer

case class DelTransfer (contig: String,start: Int,len: Int) {
  val endDel: Int = start + len
  def isOverlappingLocus(queryContig:String, queryStart:Int ): Boolean ={
    if(queryContig != contig || queryStart <= start)
      return false
    if (queryStart <= endDel)
      return true
    false
  }
}

class DelContext extends Serializable  {
  private val minDelLen: Int = 0
  val dels: ArrayBuffer[DelTransfer] = new ArrayBuffer[DelTransfer]()

  def add(delTransfer: DelTransfer):Unit = {
    if (delTransfer.len <= minDelLen)
      return
    dels.append(delTransfer)
  }

  def getDelTransferForLocus(contig:String, position: Int): Int = {
    var counter = 0
    for (del <- dels) {
      if (del.isOverlappingLocus(contig, position))
        counter += 1
    }
    counter
  }
}
