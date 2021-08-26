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

  def getAdjustedPartitionBounds(lowerBounds : Array[LowerPartitionBound]): Array[PartitionBounds] = {
    val adjPartitionBounds = new Array[PartitionBounds](lowerBounds.length)
    var i = 0
    while(i < lowerBounds.length - 1){
      adjPartitionBounds(i) =  PartitionBounds(
        lowerBounds(i).idx,
        lowerBounds(i).record.getContig,
        lowerBounds(i).record.getAlignmentStart,
        lowerBounds(i + 1).record.getContig,
        lowerBounds(i + 1).record.getAlignmentStart - 1
      )
      i += 1
    }
    val lastIdx = lowerBounds.length - 1
    adjPartitionBounds(lastIdx) = PartitionBounds(
      lowerBounds(lastIdx).idx,
      lowerBounds(lastIdx).record.getContig,
      lowerBounds(lastIdx).record.getAlignmentStart,
      "Unknown",
      Int.MaxValue
    )
    adjPartitionBounds
  }

}