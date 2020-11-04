package org.biodatageeks.sequila.pileup

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.sequila.pileup.model.{Reference, _}
import org.biodatageeks.sequila.pileup.timers.PileupTimers._
import org.biodatageeks.sequila.utils.InternalParams
import org.slf4j.{Logger, LoggerFactory}
import AggregateRDDOperations.implicits._
import AlignmentsRDDOperations.implicits._
import org.apache.spark.broadcast.Broadcast
import org.biodatageeks.sequila.pileup.conf.Conf
/**
  * Class implementing pileup calculations on set of aligned reads
  */
object PileupMethods {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  /**
    * implementation of pileup algorithm
    *
    * @param alignments aligned reads
    * @param spark spark session
    * @return distributed collection of PileupRecords
    */
  def calculatePileup(alignments: RDD[SAMRecord], spark: SparkSession, refPath: String, conf : Broadcast[Conf]): RDD[InternalRow] = {


    val enableInstrumentation = spark.sqlContext.getConf(InternalParams.EnableInstrumentation).toBoolean
    val alignmentsInstr = if(enableInstrumentation) alignments.instrument() else alignments
    val storageLevel =
      if (spark.sqlContext.getConf(InternalParams.SerializationMode, StorageLevel.MEMORY_AND_DISK.toString())==StorageLevel.DISK_ONLY.toString())
        StorageLevel.DISK_ONLY
      else StorageLevel.MEMORY_AND_DISK

    //FIXME: Add automatic unpersist
    val aggregates = ContigAggrTimer.time {alignmentsInstr.assembleContigAggregates(conf).persist(storageLevel) }
    aggregates.setName(InternalParams.RDDEventsName)
    val accumulator = AccumulatorTimer.time {aggregates.accumulateTails(spark)}

    val broadcast = BroadcastTimer.time{
      spark.sparkContext.broadcast(accumulator.value().prepareCorrectionsForOverlaps())
    }
    val adjustedEvents = AdjustedEventsTimer.time {aggregates.adjustWithOverlaps(broadcast) }
    val pileup = EventsToPileupTimer.time {adjustedEvents.toPileup(refPath, conf)}
    pileup
  }
}
