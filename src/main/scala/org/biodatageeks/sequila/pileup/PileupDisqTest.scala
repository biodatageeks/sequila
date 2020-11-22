package org.biodatageeks.sequila.pileup

import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.SparkSession
import org.disq_bio.disq.{HtsjdkReadsRddStorage, HtsjdkReadsTraversalParameters}
import htsjdk.samtools.util.Interval
import org.biodatageeks.sequila.pileup.partitioning.{PartitionUtils, RangePartitionCoalescer}

import collection.JavaConverters._


object PileupDisqTest {
  def main(args: Array[String]): Unit = {
    System.setSecurityManager(null)
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.driver.memory", "8g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
      .config("spark.driver.maxResultSize", "5g")
      //      .config("spark.kryoserializer.buffer.max", "512m")
      .enableHiveSupport()
      .getOrCreate()
    val bamPath = "/Users/mwiewior/research/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.md.bam"
    val step = 10000
    val ranges = (1 to 247249719 by step).map( p => new Interval("chr1", p, p + step)).toList
    val traverseParam = new HtsjdkReadsTraversalParameters(ranges.asJava, false)
    val reads = HtsjdkReadsRddStorage
      .makeDefault(spark.sparkContext)
      .validationStringency(ValidationStringency.LENIENT)
      .read(bamPath)
      .getReads
      .rdd

//    val a = (0 to 10).map(r=> new Integer(r )).asJava
    val a = List(1,2,3,4,5,6,7,8,9,9).map(r=> new Integer(r )).asJava
    val reads2 = reads
      .coalesce(reads.getNumPartitions,false, Some(new RangePartitionCoalescer(a )) )


//    print(ranges)
    spark.time {
//      print(reads2.count)
      val bounds = PartitionUtils.getPartitionLowerBound(reads)
      bounds.foreach(l => println(s"Partition ${l.idx} => ${l.record.getContig},${l.record.getAlignmentStart}") )
      val adjBounds = PartitionUtils.getAdjustedPartitionBounds(bounds)
      adjBounds.foreach(l => println(s"Partition ${l.idx} => ${l.contigStart},${l.postStart} -- ${l.contigEnd},${l.posEnd}") )
      val broadcastBounds = spark.sparkContext.broadcast(adjBounds)
      val cnt = reads2.mapPartitionsWithIndex{
        (i,p) => {
          val bounds = broadcastBounds.value(i)
          println(bounds)
          p.takeWhile(r =>
            if (r.getReadUnmappedFlag == true) true
            else if( r.getContig == bounds.contigStart && r.getAlignmentStart >= bounds.postStart  && r.getAlignmentEnd <= bounds.posEnd) true
            else
              false)
        }
      }.count()
      println(cnt)
    }

  }
}
