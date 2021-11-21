package org.biodatageeks.sequila.pileup

import htsjdk.samtools.SAMRecord
import htsjdk.tribble.readers.TabixReader.Iterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcOutputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.model.AggregateRDDOperations.implicits._
import org.biodatageeks.sequila.pileup.model.AlignmentsRDDOperations.implicits._
import org.biodatageeks.sequila.pileup.partitioning.PartitionBounds
import org.biodatageeks.sequila.utils.Columns
import org.slf4j.{Logger, LoggerFactory}
import org.apache.orc.TypeDescription
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{IntegerType, ShortType, StringType, StructField, StructType}

import collection.JavaConverters._

/**
  * Class implementing pileup calculations on set of aligned reads
  */
object PileupMethods {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  /**
    * implementation of pileup algorithm
    *
    * @param alignments aligned reads repartitions
    * @param spark spark session
    * @return distributed collection of PileupRecords
    */
  def calculatePileup(alignments: RDD[SAMRecord], bounds: Broadcast[Array[PartitionBounds]],
                      spark: SparkSession, refPath: String, conf : Broadcast[Conf],
                      output: Seq[Attribute]): RDD[InternalRow] = {
    val aggregates = if (conf.value.includeBaseQualities)
      alignments.assembleAggregatesWithQuals(bounds, conf)
    else
      alignments.assembleAggregates(bounds, conf)

    val result =
      if (conf.value.coverageOnly && conf.value.useVectorizedOrcWriter) {
        aggregates
          .toCoverageVectorizedWriter(bounds, output, conf)
          .count()
        spark.sparkContext.emptyRDD[InternalRow]
      }
      else if (conf.value.coverageOnly){
          aggregates.toCoverage(bounds)
      }
      else if(conf.value.useVectorizedOrcWriter) {
        aggregates.toPileupVectorizedWriter(refPath, bounds, output, conf)
          .count()
        spark.sparkContext.emptyRDD[InternalRow]
      }
      else aggregates.toPileup(refPath, bounds)
    result
  }
}
