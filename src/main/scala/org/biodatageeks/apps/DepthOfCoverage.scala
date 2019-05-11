package org.biodatageeks.apps

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.SparkSession
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.rogach.scallop.ScallopConf
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.utils.{SequilaRegister, UDFRegister,BDGInternalParams}





object DepthOfCoverage {

  case class Region(contigName:String, start:Int, end:Int)

  class RunConf(args:Array[String]) extends ScallopConf(args){

    val output = opt[String](required = true)
    val reads = opt[String](required = true)
    val format = opt[String](required = true)
    verify()
  }


  def main(args: Array[String]): Unit = {
    val runConf = new RunConf(args)
    val spark = SparkSession
      .builder()
      .appName("SeQuiLa-DoC")
      .getOrCreate()


    spark
      .sparkContext
      .setLogLevel("WARN")

    spark
      .sparkContext
      .hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)


    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)


    ss.sql(s"""CREATE TABLE IF NOT EXISTS reads  USING org.biodatageeks.datasources.BAM.BAMDataSource  OPTIONS(path '${runConf.reads()}')""")

    val sample = ss.sql(s"SELECT DISTINCT (sampleId) from reads").first().get(0)
    println(s"Input file: ${runConf.reads()}")
    println(s"Format: ${runConf.format()}")
    println(s"Sample: $sample")


    val query = "SELECT * FROM bdg_coverage('reads', '%s', '%s')".format(sample, runConf.format())


    ss.sql(query)
      .orderBy("contigName", "start")
      .coalesce(1)
      .write
        .mode("overwrite")
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(runConf.output())

    println(s"Coverage for $sample stored in ${runConf.output()}")
  }

}
