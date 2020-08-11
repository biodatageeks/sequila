package org.biodatageeks.sequila.pileup.model

import java.util

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.biodatageeks.sequila.pileup.timers.PileupTimers.{AggMapLookupTimer, AnalyzeReadsTimer, BAMReadTimer, DQTimerTimer, HandleFirstContingTimer, InitContigLengthsTimer, MapPartitionTimer, PrepareOutupTimer}
import org.biodatageeks.sequila.utils.{DataQualityFuncs, FastMath}

import scala.collection.{JavaConverters, mutable}
import ReadOperations.implicits._
import org.apache.spark.util.SizeEstimator
import org.slf4j.{Logger, LoggerFactory}
import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.pileup.model.Alts.MultiLociAlts
import org.biodatageeks.sequila.pileup.model.Quals.MultiLociQuals

object AlignmentsRDDOperations {
  object implicits {
    implicit def alignmentsRdd(rdd: RDD[SAMRecord]) = AlignmentsRDD(rdd)
  }
}

case class AlignmentsRDD(rdd: RDD[SAMRecord]) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  /**
    * Collects "interesting" (read start, stop, ref/nonref counting) events on alignments
    *
    * @return distributed collection of PileupRecords
    */
  def assembleContigAggregates: RDD[ContigAggregate] = {
    val contigLenMap = InitContigLengthsTimer.time  {
      initContigLengths(this.rdd.first())
    }
    this.rdd.mapPartitions { partition =>
//      println(s"Creating aggregates from alignments")

      val aggMap = new mutable.HashMap[String, ContigAggregate]()
      val contigMaxReadLen = new mutable.HashMap[String, Int]()
      var contigIter, contigCleanIter  = ""
      var contigAggregate: ContigAggregate = null
      MapPartitionTimer.time {
        while (partition.hasNext) {
          val read = BAMReadTimer.time {partition.next()}
          val contig = DQTimerTimer.time {
            if(read.getContig == contigIter)
              contigCleanIter
            else {
              contigIter = read.getContig
              contigCleanIter =  DataQualityFuncs.cleanContig(contigIter)
              contigCleanIter
            }
          }

          if (!aggMap.contains(contig))
            HandleFirstContingTimer.time {
              handleFirstReadForContigInPartition(read, contig, contigLenMap, contigMaxReadLen, aggMap)
              contigAggregate = AggMapLookupTimer.time {aggMap(contig) }
            }
          AnalyzeReadsTimer.time {read.analyzeRead(contig, contigAggregate, contigMaxReadLen)}
        }
        val aggregates = PrepareOutupTimer.time {prepareOutputAggregates(aggMap, contigMaxReadLen).toIterator}
        aggregates
      }
    }
  }


  /**
    * transforms map structure of contigEventAggregates, by reducing number of last zeroes in the cov array
    * also adds calculated maxCigar len to output
    *
    * @param aggMap   mapper between contig and contigEventAggregate
    * @param cigarMap mapper between contig and max length of cigar in given
    * @return
    */
  def prepareOutputAggregates(aggMap: mutable.HashMap[String, ContigAggregate], cigarMap: mutable.HashMap[String, Int]): Array[ContigAggregate] = {
    //println(s"Preparing output aggregates")
    val output = new Array[ContigAggregate](aggMap.size)
    var i = 0
    val iter = aggMap.toIterator
    while(iter.hasNext){
      val nextVal = iter.next()
      val contig = nextVal._1
      val contigEventAgg = nextVal._2

      val maxIndex: Int = FastMath.findMaxIndex(contigEventAgg.events)
      val agg = ContigAggregate(
        contig,
        contigEventAgg.contigLen,
        util.Arrays.copyOfRange(contigEventAgg.events, 0, maxIndex + 1), //FIXME: https://stackoverflow.com/questions/37969193/why-is-array-slice-so-shockingly-slow
        contigEventAgg.alts,
        contigEventAgg.quals,
        contigEventAgg.startPosition,
        contigEventAgg.startPosition + maxIndex,
        0,
        cigarMap(contig),
        contigEventAgg.qualityCache)
//      val coef = 1048576.0
//      val aggSize = SizeEstimator.estimate(agg)/coef
//      val altsSize = SizeEstimator.estimate(agg.alts)/coef
//      val qualSize = SizeEstimator.estimate(agg.quals)/coef
//      val cacheSize = SizeEstimator.estimate(agg.qualityCache)/coef
//      val altsKeySize = SizeEstimator.estimate(agg.alts.keySet)/coef
//      val altsValuesSize = SizeEstimator.estimate(agg.alts.values.toSet)/coef
//      val qualsKeySize = SizeEstimator.estimate(agg.quals.keySet)/coef
//      val qualsValueSize = SizeEstimator.estimate(agg.quals.values.toSet)/coef

      output(i) = agg


      i += 1
    }
    output
  }


  private def handleFirstReadForContigInPartition(read: SAMRecord, contig: String, contigLenMap: Map[String, Int],
                                                  contigMaxReadLen: mutable.HashMap[String, Int],
                                                  aggMap: mutable.HashMap[String, ContigAggregate]
                                                  ):Unit = {
    val contigLen = contigLenMap(contig)
    val arrayLen = contigLen - read.getStart + 10

    val contigEventAggregate = ContigAggregate(
      contig = contig,
      contigLen = contigLen,
      events = new Array[Short](arrayLen),
      alts = new MultiLociAlts(),
      quals = if(Conf.includeBaseQualities ) new MultiLociQuals() else null,
      startPosition = read.getStart,
      maxPosition = contigLen - 1,
      qualityCache =  if(Conf.includeBaseQualities ) new QualityCache(QualityConstants.CACHE_SIZE) else null)

    aggMap += contig -> contigEventAggregate
    contigMaxReadLen += contig -> 0
  }

  /**
    * initializes mapper between contig and its length basing on header values
    *
    * @param read single aligned read (its header contains info about all contigs)
    * @return
    */
  def initContigLengths(read: SAMRecord): Map[String, Int] = {
    val contigLenMap = new mutable.HashMap[String, Int]()
    val sequenceList = read.getHeader.getSequenceDictionary.getSequences
    val sequenceSeq = JavaConverters.asScalaIteratorConverter(sequenceList.iterator()).asScala.toSeq

    for (sequence <- sequenceSeq) {
      val contigName = DataQualityFuncs.cleanContig(sequence.getSequenceName)
      contigLenMap += contigName -> sequence.getSequenceLength
    }
    contigLenMap.toMap
  }

}