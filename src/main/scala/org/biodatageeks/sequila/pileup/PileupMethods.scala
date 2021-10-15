package org.biodatageeks.sequila.pileup

import htsjdk.samtools.SAMRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.model.AggregateRDDOperations.implicits._
import org.biodatageeks.sequila.pileup.model.AlignmentsRDDOperations.implicits._
import org.biodatageeks.sequila.pileup.partitioning.PartitionBounds
import org.slf4j.{Logger, LoggerFactory}
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
  def calculatePileup(alignments: RDD[SAMRecord], bounds: Broadcast[Array[PartitionBounds]], spark: SparkSession, refPath: String, conf : Broadcast[Conf]): RDD[InternalRow] = {
    val aggregates = alignments.assembleContigAggregates(conf)
    val pileup = aggregates.toPileup(refPath, bounds)
    pileup
  }
}
