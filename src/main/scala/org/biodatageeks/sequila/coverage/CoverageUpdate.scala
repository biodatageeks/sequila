package org.biodatageeks.sequila.coverage

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

case class RightCovEdge(contig: String,
                        minPos: Int,
                        startPoint: Int,
                        cov: Array[Short],
                        cumSum: Short)

case class ContigRange(contig: String, minPos: Int, maxPos: Int)

class CovUpdate(var right: ArrayBuffer[RightCovEdge],
                var left: ArrayBuffer[ContigRange])
    extends Serializable {

  def reset(): Unit = {
    right = new ArrayBuffer[RightCovEdge]()
    left = new ArrayBuffer[ContigRange]()
  }
  def add(p: CovUpdate): CovUpdate = {
    right = right ++ p.right
    left = left ++ p.left
    this
  }

}

class CoverageAccumulatorV2(var covAcc: CovUpdate)
    extends AccumulatorV2[CovUpdate, CovUpdate] {

  def reset(): Unit = {
    covAcc = new CovUpdate(new ArrayBuffer[RightCovEdge](),
                           new ArrayBuffer[ContigRange]())
  }

  def add(v: CovUpdate): Unit = {
    covAcc.add(v)
  }
  def value(): CovUpdate = {
    covAcc
  }
  def isZero(): Boolean = {
    covAcc.right.isEmpty && covAcc.left.isEmpty
  }
  def copy(): CoverageAccumulatorV2 = {
    new CoverageAccumulatorV2(covAcc)
  }
  def merge(other: AccumulatorV2[CovUpdate, CovUpdate]): Unit = {
    covAcc.add(other.value)
  }
}
