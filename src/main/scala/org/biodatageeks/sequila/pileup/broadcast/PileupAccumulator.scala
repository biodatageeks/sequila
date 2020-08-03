package org.biodatageeks.sequila.pileup.broadcast

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.ArrayBuffer


class PileupAccumulator(var pilAcc: PileupUpdate)
  extends AccumulatorV2[PileupUpdate, PileupUpdate] {

  def reset(): Unit = {
    pilAcc = new PileupUpdate(new ArrayBuffer[Tail](),
      new ArrayBuffer[Range]())
  }

  def add(v: PileupUpdate): Unit = {
    pilAcc.add(v)
  }
  def value(): PileupUpdate = {
    pilAcc
  }
  def isZero(): Boolean = {
    pilAcc.tails.isEmpty && pilAcc.ranges.isEmpty
  }
  def copy(): PileupAccumulator = {
    new PileupAccumulator(pilAcc)
  }
  def merge(other: AccumulatorV2[PileupUpdate, PileupUpdate]): Unit = {
    pilAcc.add(other.value)
  }
}