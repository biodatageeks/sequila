package org.biodatageeks.sequila.apps

import htsjdk.samtools.{SAMRecord, ValidationStringency}
import org.apache.spark.sql.SparkSession
import org.disq_bio.disq.{HtsjdkReadsRddStorage, HtsjdkReadsTraversalParameters}
import htsjdk.samtools.util.Interval
import org.apache.spark.rdd.RDD
import org.biodatageeks.sequila.pileup.partitioning.{PartitionUtils, RangePartitionCoalescer}
import org.biodatageeks.sequila.utils.InternalParams

import collection.JavaConverters._

object PileupDisqTest {
  val splitSize = "1000000"
  val bamPath = "/Users/aga/workplace/data/NA12878.chr20.md.bam"
  def main(args: Array[String]): Unit = {
    System.setSecurityManager(null)
    val spark = SparkSession.builder().master("local[1]").config("spark.driver.memory", "8g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
      .config("spark.driver.maxResultSize", "5g").getOrCreate()

    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val reads = HtsjdkReadsRddStorage
      .makeDefault(spark.sparkContext)
      .validationStringency(ValidationStringency.LENIENT).read(bamPath).getReads.rdd
    //showReadsRDDSummary(reads, spark)

    val numPartitions = reads.getNumPartitions
    val maxEndIndex = (List.range(1, numPartitions) :+ (numPartitions-1)).map(r=> new Integer(r )).asJava

    val reads2 = reads.coalesce(reads.getNumPartitions,false, Some(new RangePartitionCoalescer(maxEndIndex )) )
    //showReadsRDDSummary(reads2, spark)

    val lowerBounds = PartitionUtils.getPartitionLowerBound(reads) // get the start of first read in partition
    lowerBounds.foreach(l => println(s"Partition lower lowerBounds ${l.idx} => ${l.record.getContig},${l.record.getAlignmentStart}"))
    val adjBounds = PartitionUtils.getAdjustedPartitionBounds(lowerBounds)
    adjBounds.foreach(l => println(s"Partition ${} => ${l.contigStart},${l.postStart} -- ${l.contigEnd},${l.posEnd}"))
    val broadcastBounds = spark.sparkContext.broadcast(adjBounds)
    val cnt = reads2.mapPartitionsWithIndex {
      (i, p) => {
        val bounds = broadcastBounds.value(i)
        p.takeWhile(r =>
          if (r.getReadUnmappedFlag) true
          else if (r.getContig == bounds.contigStart && r.getAlignmentStart >= bounds.postStart && r.getAlignmentEnd <= bounds.posEnd) true
          else
            false)
      }
    }.count()
    //println(cnt)
    //println(reads.filter(r=>r.getReadUnmappedFlag).count())
  }

  def showReadsRDDSummary(reads: RDD[SAMRecord], spark: SparkSession): Unit = {
    import spark.implicits._
    println(s"Partitions ${reads.getNumPartitions} and count ${reads.count()}")
    reads.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.toDF("partition_number","number_of_records").show()
  }

// this was not used
  //     val step = 10000
  //val ranges = (1 to 62435964 by step).map( p => new Interval("chr20", p, p + step)).toList
  //println (ranges.head())
  //val traverseParam = new HtsjdkReadsTraversalParameters(ranges.asJava, false)
}
