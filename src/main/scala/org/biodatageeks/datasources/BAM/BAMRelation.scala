package org.biodatageeks.datasources.BAM

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.apps.FeatureCounts.Region
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}


case class BAMRecord(sampleId: String,
                     contigName:String,
                     start:Int,
                     end:Int,
                     cigar:String,
                     quality:Int,
                     basequality: String,
                     reference:String,
                     flags:Int,
                     mateReferenceIndex:Int)

class BAMRelation (path:String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  val spark = sqlContext
    .sparkSession
  spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil

  spark
    .sparkContext
    .hadoopConfiguration
    .set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)

  override def schema: org.apache.spark.sql.types.StructType = {
    StructType(
      Seq(
        new StructField("sampleId", StringType),
        new StructField("contigName", StringType),
        new StructField("start", IntegerType),
        new StructField("end", IntegerType),
        new StructField("cigar", StringType),
        new StructField("mapq", IntegerType),
        new StructField("baseq", StringType),
        new StructField("reference", StringType),
        new StructField("flags", IntegerType),
        new StructField("materefind", IntegerType)

      )
    )
  }

  override def buildScan: RDD[Row] = {

    val alignments = spark
      .sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](path)
    val alignmentsWithFileName = alignments.asInstanceOf[NewHadoopRDD[LongWritable, SAMRecordWritable]]
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
        val file = inputSplit.asInstanceOf[FileVirtualSplit]
        iterator.map(tup => (file.getPath.getName.split('.')(0), tup._2))
        }
      )
    val sampleAlignments = alignmentsWithFileName
      .map(r => (r._1, r._2.get()))
      .map { case (sampleId, r) =>
        BAMRecord(sampleId,r.getContig, r.getStart, r.getEnd, r.getCigar.toString,
          r.getMappingQuality, r.getBaseQualityString, r.getReferenceName,
          r.getFlags, r.getMateReferenceIndex)
      }


//     val alignments = spark
//      .sparkContext.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](path)
//      .map(_._2.get)
//      .map(r => BAMRecord(r.getContig, r.getStart, r.getEnd,r.getCigar.toString,
//        r.getMappingQuality, r.getBaseQualityString, r.getReferenceName,
//        r.getFlags, r.getMateReferenceIndex))

    val readsTable = spark
      .sqlContext
      .createDataFrame(sampleAlignments)
    readsTable
      .rdd

  }
}