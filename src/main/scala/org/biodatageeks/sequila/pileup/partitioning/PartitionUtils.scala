package org.biodatageeks.sequila.pileup.partitioning

import htsjdk.samtools.SAMRecord
import htsjdk.samtools.util.Interval
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.biodatageeks.sequila.pileup.TruncRead
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalHolderChromosome

import collection.JavaConverters._

case class LowerPartitionBoundAlignmentRecord(idx: Int, record: SAMRecord)
case class PartitionBounds(idx: Int, contigStart: String, postStart: Int, contigEnd: String, posEnd: Int, readName: Option[String] = None )
object PartitionUtils {

  val logger =  Logger.getLogger(this.getClass.getCanonicalName)
  val intervalStep = 1000

  def getPartitionLowerBound(rdd: RDD[SAMRecord]) : Array[LowerPartitionBoundAlignmentRecord] = {
    rdd.mapPartitionsWithIndex{
      (i, p) => Iterator(LowerPartitionBoundAlignmentRecord(i ,p.next()) )
    }.collect()
  }

  def getAdjustedPartitionBounds2(lowerBounds : Array[LowerPartitionBoundAlignmentRecord], tree: IntervalHolderChromosome[TruncRead] ):  Array[PartitionBounds] = {
    val adjPartitionBounds = new Array[PartitionBounds](lowerBounds.length)
    var i = 0
    while(i < lowerBounds.length - 1){
      val upperContig = lowerBounds(i + 1).record.getContig
      val upperPosBound = lowerBounds(i + 1).record.getAlignmentStart
      val upperReadName = lowerBounds(i + 1).record.getReadName
      val treeContig = tree.getIntervalTreeByChromosome(upperContig)
      var rName : Option[String] = None
      val maxPos = treeContig match {
        case Some(p) => {
          val reads = p.overlappers(upperPosBound, upperPosBound)
            .asScala
            .flatMap(r => r.getValue.asScala)
            .toArray
//          reads.foreach(r => println(r.rName))
          val maxOverlaps = reads //FIXME: Check if order is preserved!!!
          if (maxOverlaps.isEmpty)
            upperPosBound -1
          else {
            var maxPos = Int.MinValue
            var maxId = 0
            while(maxId < maxOverlaps.length){
              if(maxPos <= maxOverlaps(maxId).posEnd) {
                maxPos = maxOverlaps(maxId).posEnd
              }
              maxId += 1
            }
            val maxOverlap = maxOverlaps(maxId - 1)
            logger.info(s"Found max overlap for partition ${i} with read name ${maxOverlap.rName}, ${upperContig} and max position ${maxOverlap.posEnd}")
            rName = Some(maxOverlap.rName)
            maxOverlap.posEnd
          }
        }
        case _ => {
          logger.info(s"No overlaps found for partition ${i} taking ${upperContig} ${upperPosBound - 1} as max")
          upperPosBound - 1
        }
      }
      adjPartitionBounds(i) =  PartitionBounds(
        lowerBounds(i).idx,
        lowerBounds(i).record.getContig,
        lowerBounds(i).record.getAlignmentStart,
        upperContig,
        maxPos,
        rName
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


  def getAdjustedPartitionBounds(lowerBounds : Array[LowerPartitionBoundAlignmentRecord]): Array[PartitionBounds] = {
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