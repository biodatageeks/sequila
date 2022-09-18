package org.biodatageeks.sequila.rangejoins.exp.iit

import org.biodatageeks.sequila.rangejoins.methods.base.BaseNode

import java.util

class Node[V1](val start: Int, val end: Int) extends BaseNode[V1] with Serializable {
  private[iit] var max: Int = end
  private val values: util.ArrayList[V1] = new util.ArrayList[V1]()

  def this(start: Int, end: Int, values: util.ArrayList[V1]) {
    this(start, end)
    this.values.addAll(values)
  }

  override def getStart: Int = start

  override def getEnd: Int = end

  override def getValue: util.ArrayList[V1] = values

  def getMax: Int = max

  def addValue(v: V1): V1 = {
    values.add(v)
    v
  }
}
