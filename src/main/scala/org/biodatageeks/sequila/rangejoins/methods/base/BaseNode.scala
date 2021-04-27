package org.biodatageeks.sequila.rangejoins.methods.base

import java.util

abstract class BaseNode[V] extends Serializable {

  def getStart: Int
  def getEnd: Int
  def getValue : util.ArrayList[V]
}
