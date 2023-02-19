package org.biodatageeks.sequila.apps

import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.utils.InternalParams
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

trait SequilaApp {
  def createSequilaSession(): SequilaSession = {
    System.setProperty("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.driver.memory","4g")
      .getOrCreate()

    val ss = SequilaSession(spark)
    spark.sparkContext.setLogLevel("WARN")
    ss
  }

  def createSparkSessionWithExtraStrategy(sparkSave: Boolean = false): SparkSession = {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local[4]")
      .config(InternalParams.saveAsSparkFormat, sparkSave)
      .getOrCreate()

    spark.sqlContext.setConf(InternalParams.useJoinOrder, "true")
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    spark
      .sparkContext
      .setLogLevel("OFF")
    spark
      .sparkContext
      .hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)

    spark
  }
}
