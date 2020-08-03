package org.biodatageeks.sequila.pileup.broadcast

import org.biodatageeks.sequila.pileup.broadcast.Correction.PartitionCorrections
import org.biodatageeks.sequila.pileup.broadcast.Shrink.PartitionShrinks
import scala.collection.mutable.ArrayBuffer


class PileupUpdate(var tails: ArrayBuffer[Tail], var ranges: ArrayBuffer[Range]) extends Serializable {

  def reset(): Unit = {
    tails = new ArrayBuffer[Tail]()
    ranges = new ArrayBuffer[Range]()
  }

  def add(p: PileupUpdate): PileupUpdate = {
    tails = tails ++ p.tails
    ranges = ranges ++ p.ranges
    this
  }

  def prepareCorrectionsForOverlaps(): FullCorrections = { //prepare correctionsfor overlaps
    val correctionsMap = new PartitionCorrections()
    val shrinksMap = new PartitionShrinks()

    var it = 0
    for (range <- ranges.sortBy(r => (r.contig, r.minPos))) {

      val overlaps = range.findOverlaps(tails)
      val cumSum = range.precedingCumulativeSum(tails)

      if(overlaps.isEmpty)
        correctionsMap += (range.contig, range.minPos) -> Correction(None, None, None, cumSum, null)
      else { // if there are  overlaps for this contigRange
        for(o <- overlaps) {
          val overlapLength = range.calculateOverlapLength(o, it, ranges)
          correctionsMap.updateWithOverlap(o, range, overlapLength, cumSum)
          shrinksMap.updateWithOverlap(o,range)
        }
      }
      it += 1
    }
    FullCorrections(correctionsMap, shrinksMap)
  }


}




