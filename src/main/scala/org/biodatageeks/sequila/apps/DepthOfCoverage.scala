package org.biodatageeks.sequila.apps

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.rogach.scallop.ScallopConf
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister, UDFRegister}





object DepthOfCoverage {

  case class Region(contig:String, start:Int, end:Int)

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
      .setLogLevel("ERROR")

    spark
      .sparkContext
      .hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)


    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)


    ss.sql(s"""CREATE TABLE IF NOT EXISTS reads_tmp  USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource  OPTIONS(path '${runConf.reads()}')""")

    val sample = ss.sql(s"SELECT DISTINCT (${Columns.SAMPLE}) from reads_tmp").first().get(0)


    val query = "SELECT * FROM coverage('reads_tmp', '%s', '%s')".format(sample, runConf.format())

    ss.sql(query)
      .orderBy(Columns.CONTIG,"start")
      .coalesce(1)
      .write
        .mode("overwrite")
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(runConf.output())

    println(s"Coverage for $sample stored in ${runConf.output()}")
  }

}
