package org.biodatageeks.sequila.apps

import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.rogach.scallop.ScallopConf
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

object FeatureCountsFunc {

  class RunConf(args:Array[String]) extends ScallopConf(args){

    val output = opt[String](required = true)
    val annotations = opt[String](required = true)
    val readsFile = trailArg[String](required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val runConf = new RunConf(args)
    val spark = SparkSession
      .builder()
      .appName("SeQuiLa-DoC")
      .config("spark.master", "local[4]")
      .getOrCreate()

    spark
      .sparkContext
      .setLogLevel("ERROR")

    spark
      .sparkContext
      .hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)

    val ss = SequilaSession(spark)

    val query = s"Select fc.*" +
      s" FROM feature_counts('${runConf.readsFile()}', '${runConf.annotations()}') fc "

    ss.sql(query)
      .orderBy("sample_id")
      .coalesce(1)
      .show()
  }
}
