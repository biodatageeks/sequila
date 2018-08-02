package org.biodatageeks.preprocessing.coverage

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.AccumulatorV2

case class RightCovEdge(contigName:String,minPos:Int,startPoint:Int,cov:Array[Short], cumSum:Short)

case class ContigRange(contigName:String, minPos: Int, maxPos:Int)
class CovUpdate(var right:ArrayBuffer[RightCovEdge],var left: ArrayBuffer[ContigRange]) extends Serializable {

  def reset(): Unit = {
    right = new ArrayBuffer[RightCovEdge]()
    left = new ArrayBuffer[ContigRange]()
  }
  def add(p:CovUpdate): CovUpdate = {
    right.append(p.right.head)
    left.append(p.left.head)

    return this
  }

}

class CoverageAccumulatorV2(var covAcc: CovUpdate) extends AccumulatorV2[CovUpdate, CovUpdate] {
  //private val covAcc = new CovUpdate(new ArrayBuffer[RightCovEdge](),new ArrayBuffer[ContigRange]())

  def reset(): Unit = {
    covAcc =  new CovUpdate(new ArrayBuffer[RightCovEdge](),new ArrayBuffer[ContigRange]())
  }

  def add(v: CovUpdate): Unit = {
    covAcc.add(v)
  }
  def value():CovUpdate = {
    return covAcc
  }
  def isZero(): Boolean = {
    return (covAcc.right.isEmpty && covAcc.left.isEmpty)
  }
  def copy():CoverageAccumulatorV2 = {
    return new CoverageAccumulatorV2 (covAcc)
  }
  def merge(other:AccumulatorV2[CovUpdate, CovUpdate]) = {
    covAcc.add(other.value)
  }
}