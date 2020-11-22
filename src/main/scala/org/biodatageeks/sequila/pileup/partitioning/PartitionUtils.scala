package org.biodatageeks.sequila.pileup.partitioning

import htsjdk.samtools.SAMRecord
import htsjdk.samtools.util.Interval
import org.apache.spark.rdd.RDD

case class LowerPartitionBound(idx: Int, record: SAMRecord)
case class PartitionBounds(idx: Int, contigStart: String, postStart: Int, contigEnd: String, posEnd: Int)
object PartitionUtils {

  val intervalStep = 1000

  def getPartitionLowerBound(rdd: RDD[SAMRecord]) : Array[LowerPartitionBound] = {
    rdd.mapPartitionsWithIndex{
      (i, p) => Iterator(LowerPartitionBound(i ,p.next()) )
    }.collect()
  }

  def getAdjustedPartitionBounds(p : Array[LowerPartitionBound]) = {
    val adjPartitionBounds = new Array[PartitionBounds](p.size)
    var i = 0
    while(i < p.size - 1){
      adjPartitionBounds(i) =  PartitionBounds(
          p(i).idx,
          p(i).record.getContig,
          p(i).record.getAlignmentStart,
          p(i + 1).record.getContig,
          p(i + 1).record.getAlignmentStart - 1
        )
      i += 1
    }
    val lastIdx = p.size - 1
    adjPartitionBounds(lastIdx) = PartitionBounds(
      p(lastIdx).idx,
      p(lastIdx).record.getContig,
      p(lastIdx).record.getAlignmentStart,
      "Uknown",
      Int.MaxValue
    )
    adjPartitionBounds
  }

}
