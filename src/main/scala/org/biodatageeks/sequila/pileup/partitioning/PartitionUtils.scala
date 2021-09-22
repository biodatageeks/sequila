package org.biodatageeks.sequila.pileup.partitioning

import htsjdk.samtools.SAMRecord
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.biodatageeks.sequila.pileup.TruncRead
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalHolderChromosome

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class LowerPartitionBoundAlignmentRecord(idx: Int, record: SAMRecord)
case class PartitionBounds(idx: Int, contigStart: String, postStart: Int,
                           contigEnd: String, posEnd: Int,
                           readName: Option[String] = None ,
                           wholeContigs: Set[String] = Set[String]())
object PartitionUtils {

  val logger =  Logger.getLogger(this.getClass.getCanonicalName)
  val intervalStep = 1000

  def getPartitionLowerBound(rdd: RDD[SAMRecord]) : Array[LowerPartitionBoundAlignmentRecord] = {
    rdd.mapPartitionsWithIndex{
      (i, p) => Iterator(LowerPartitionBoundAlignmentRecord(i ,p.next()) )
    }.collect()
  }

  def getAdjustedPartitionBounds(lowerBounds : Array[LowerPartitionBoundAlignmentRecord],
                                 tree: IntervalHolderChromosome[TruncRead],
                                 conf: Conf, contigsList: Array[String]):  Array[PartitionBounds] = {
    val adjPartitionBounds = new Array[PartitionBounds](lowerBounds.length)
    var i = 0
    var previousMaxPos = Int.MinValue
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
          if(i ==0) lowerBounds(i).record.getAlignmentStart else previousMaxPos + 1,
          upperContig,
          maxPos,
          rName,
          getContigsBetween(lowerBounds(i).record.getContig, upperContig, contigsList )
        )
      i += 1
      previousMaxPos = maxPos
    }
    val lastIdx = lowerBounds.length - 1
    adjPartitionBounds(lastIdx) = PartitionBounds(
      lowerBounds(lastIdx).idx,
      lowerBounds(lastIdx).record.getContig,
      if(lastIdx > 0 && lowerBounds(lastIdx).record.getContig == lowerBounds(lastIdx - 1).record.getContig)
        previousMaxPos + 1
      else
        lowerBounds(lastIdx).record.getAlignmentStart,
      conf.unknownContigName,
      Int.MaxValue
    )
    adjPartitionBounds
  }

private def getContigsBetween(startContig: String, endContig: String, contigsList: Array[String]) = {
  contigsList
    .slice(
      contigsList.indexOf(startContig) + 1,
      contigsList.indexOf(endContig)
    ).toSet
}

  def getMaxEndPartitionIndex(adjBounds: Array[PartitionBounds], lowerBounds: Array[LowerPartitionBoundAlignmentRecord]) = {
    adjBounds.map(
      r => {
        val maxPos = r.posEnd
        val maxIndex = lowerBounds.takeWhile( p => p.record.getAlignmentStart <= maxPos).takeRight(1)(0).idx
        maxIndex
      }
    ).toList
  }

//  def getAdjustedPartitionBounds(lowerBounds : Array[LowerPartitionBoundAlignmentRecord]): Array[PartitionBounds] = {
//    val adjPartitionBounds = new Array[PartitionBounds](lowerBounds.length)
//    var i = 0
//    while(i < lowerBounds.length - 1){
//      adjPartitionBounds(i) =  PartitionBounds(
//        lowerBounds(i).idx,
//        lowerBounds(i).record.getContig,
//        lowerBounds(i).record.getAlignmentStart,
//        lowerBounds(i + 1).record.getContig,
//        lowerBounds(i + 1).record.getAlignmentStart - 1
//      )
//      i += 1
//    }
//    val lastIdx = lowerBounds.length - 1
//    adjPartitionBounds(lastIdx) = PartitionBounds(
//      lowerBounds(lastIdx).idx,
//      lowerBounds(lastIdx).record.getContig,
//      lowerBounds(lastIdx).record.getAlignmentStart,
//      "Unknown",
//      Int.MaxValue
//    )
//    adjPartitionBounds
//  }



}