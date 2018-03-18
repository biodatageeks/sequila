package org.biodatageeks.datasources.BAM

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.apps.FeatureCounts.Region
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}


case class BAMRecord(contigName:String,start:Int,end:Int,cigar:String)

class BAMRelation (path:String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

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
        new StructField("contigName", StringType),
        new StructField("start", IntegerType),
        new StructField("end", IntegerType),
        new StructField("cigar", StringType)

      )
    )
  }
  override def buildScan(requiredColumns: Array[String],  filters: Array[Filter]): RDD[Row] = {
     val alignments = spark
      .sparkContext.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](path)
      .map(_._2.get)
      .map(r => BAMRecord(r.getContig, r.getStart, r.getEnd,r.getCigar.toString))

    val readsTable = spark.sqlContext.createDataFrame(alignments)
    readsTable
      .rdd

  }
}