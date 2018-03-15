package org.biodatageeks.apps

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.SparkSession
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.rogach.scallop.ScallopConf
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

object FeatureCounts {
  case class Region(contigName:String,start:Int,end:Int)
  class RunConf(args:Array[String]) extends ScallopConf(args){

    val output = opt[String](required = true)
    val annotations = opt[String](required = true)
    val readsFile = trailArg[String](required = true)
    val Format = trailArg[String](required = false)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val runConf = new RunConf(args)
    val spark = SparkSession
      .builder()
      .appName("SeQuiLa-FC")
      .getOrCreate()

    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder","true")
    //spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (1024).toString)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil



    val query ="""SELECT targets.GeneId AS GeneId,
                     targets.Chr AS Chr,
                     targets.Start AS Start,
                     targets.End AS End,
                     targets.Strand AS Strand,
                     CAST(targets.End AS INTEGER)-CAST(targets.Start AS INTEGER) + 1 AS Length,
                     count(*) AS Counts
            FROM reads JOIN targets
      |ON (
      |  targets.Chr=reads.contigName
      |  AND
      |  reads.end >= CAST(targets.Start AS INTEGER)
      |  AND
      |  reads.start <= CAST(targets.End AS INTEGER)
      |)
      |GROUP BY targets.GeneId,targets.Chr,targets.Start,targets.End,targets.Strand""".stripMargin
      spark
        .sparkContext
        .setLogLevel("WARN")

      spark
        .sparkContext
        .hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)

      val alignments = spark
        .sparkContext.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](runConf.readsFile())
        .map(_._2.get)
        .map(r => Region(r.getContig, r.getStart, r.getEnd))

      val readsTable = spark.sqlContext.createDataFrame(alignments)
      readsTable.createOrReplaceTempView("reads")

      val targets = spark
        .read
        .option("header", "true")
        .option("delimiter", "\t")
        .csv(runConf.annotations())
      targets.createOrReplaceTempView("targets")

     spark.sql(query)
       .orderBy("GeneId")
        .coalesce(1)
        .write
        .option("header", "true")
        .option("delimiter", "\t")
        .csv(runConf.output())
  }

}
