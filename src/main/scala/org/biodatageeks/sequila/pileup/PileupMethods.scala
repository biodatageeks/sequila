package org.biodatageeks.sequila.pileup

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.iceberg.data.orc.GenericOrcWriter
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
import org.apache.iceberg.orc.{ORC, OrcRowWriter}
import org.apache.iceberg.{Files, Schema}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.data.SparkOrcWriter
import org.apache.orc.TypeDescription
import org.apache.spark.sql.types.{IntegerType, ShortType, StringType, StructField, StructType}

import scala.::

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
  def calculatePileup(alignments: RDD[SAMRecord], bounds: Broadcast[Array[PartitionBounds]], spark: SparkSession, refPath: String, conf : Broadcast[Conf]): RDD[InternalRow] = {
    val aggregates = if (conf.value.includeBaseQualities)
      alignments.assembleAggregatesWithQuals(bounds, conf)
    else
      alignments.assembleAggregates(bounds, conf)

    val result =
      if (conf.value.coverageOnly) {
//        val conf = new Configuration()
        val schema = s"struct<${Columns.CONTIG}:string,${Columns.START}:int,${Columns.END}:int,${Columns.REF}:String,${Columns.COVERAGE}:smallint>"
//        conf.set("orc.mapred.output.schema", schema )
//        conf.set("orc.compress", "SNAPPY")
//        aggregates
//          .toCoverageFastMode(bounds)
//        .saveAsNewAPIHadoopFile(
//          "/tmp/test.orc",
//          classOf[NullWritable],
//          classOf[OrcStruct],
//          classOf[OrcOutputFormat[OrcStruct]],
//          conf
//        )
        val records = aggregates.toCoverage(bounds)
        val basicOutput = StructType(Seq(
          StructField(Columns.CONTIG,StringType,nullable = true),
          StructField(Columns.START,IntegerType,nullable = false),
          StructField(Columns.END,IntegerType,nullable = false),
          StructField(Columns.REF,StringType,nullable = false),
          StructField(Columns.COVERAGE,ShortType,nullable = false)
        ))
        val icebergSchema = SparkSchemaUtil.convert(basicOutput)
        val writer = ORC
          .write(Files.localOutput("/tmp/test_vect"))
          .createWriterFunc(GenericOrcWriter.buildWriter)
          .schema(icebergSchema)
          .build()

        spark.sparkContext.emptyRDD[InternalRow]

      } else
        aggregates.toPileup(refPath, bounds)
    result
  }
}
