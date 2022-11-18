package org.biodatageeks.sequila.apps

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.utils.{Columns, InternalParams}
import org.rogach.scallop.ScallopConf
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}

object FeatureCounts {
  case class Region(contig:String, pos_start:Int, pos_end:Int)
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
      .config("spark.master", "local[4]")
      .getOrCreate()

    spark.sqlContext.setConf(InternalParams.useJoinOrder,"true")
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
      |  targets.Chr=reads.contig
      |  AND
      |  reads.pos_end >= CAST(targets.Start AS INTEGER)
      |  AND
      |  reads.pos_start <= CAST(targets.End AS INTEGER)
      |)
      |GROUP BY targets.GeneId,targets.Chr,targets.Start,targets.End,targets.Strand""".stripMargin
      spark
        .sparkContext
        .setLogLevel("ERROR")

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
      targets
        .withColumnRenamed("contig", Columns.CONTIG)
        .createOrReplaceTempView("targets")

     spark.sql(query)
       .orderBy("GeneId")
        .coalesce(1)
        .write
        .option("header", "true")
        .option("delimiter", "\t")
        .csv(runConf.output())
  }

}
