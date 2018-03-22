package org.biodatageeks.rangejoins.common.metrics

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.StructType
import org.bdgenomics.adam.rdd.ADAMContext._


case class TestMetaRecord(testId:String,
                      algorithm:String,
                      timestamp: Timestamp,
                      executorNum: Option[Int],
                      coresPerExecutor: Option[Int],
                      executorMemoryGb: Option[Int],
                      driverMemoryGb:Option[Int],
                      tableNames: Array[String],
                      tableRowcounts: Array[Long],
                      executionTimeSec: Double
                     )

class MetricsCollector( sparkSession: SparkSession, metricsTableName: String) {

  val spark = sparkSession

  private val columnsToDrop = Array("jobId","stageId","name","submissionTime", "completionTime")

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    val duration = (System.nanoTime - s) / 1e9
    println("time: " + duration  + " seconds")
    (ret,duration)
  }

  private def getMetricsTableSchema = {

    val metaSchema = ScalaReflection.schemaFor[TestMetaRecord].dataType.asInstanceOf[StructType]
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    val metrics = stageMetrics.createStageMetricsDF()
    val aggMetrics = metrics
      .drop(columnsToDrop: _*)
    val cleanColumnNames = aggMetrics
      .columns
      .map(r=>r
        .replace(")","")
        .replace("(","")
      )
    val mergedSchema = StructType(metaSchema ++ aggMetrics.toDF(cleanColumnNames: _*).schema)
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], mergedSchema)
  }

  def initMetricsTable = {

    val df = getMetricsTableSchema
    df.createOrReplaceTempView("metrics_temp")
    spark.sql(
      s"""
        |CREATE TABLE IF NOT EXISTS  ${metricsTableName} STORED AS PARQUET
        |AS SELECT * FROM metrics_temp
      """.stripMargin)

//    spark.sql(s"DESC FORMATTED ${metricsTableName}")
//      .show(1000,false)

  }

  def dropMetricsTable = {
    spark.sql(
      s"""
         |DROP TABLE IF EXISTS  ${metricsTableName}
      """.stripMargin)

  }



  def runAndCollectMetrics(queryId:String,algoName:String,tables:Array[String],query:String, saveOutput:Boolean = false) = {

    val arraysCount = new Array[Long](tables.size)
    for(i<- 0 to arraysCount.size - 1 ){
      arraysCount(i) =
        spark.sql(s"SELECT count(*) FROM ${tables(i)}")
        .first()
        .getLong(0)
    }
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    val executionTime = saveOutput match {
      case false => time(stageMetrics.runAndMeasure(spark.sql(query).count()))._2
      case _ => time(stageMetrics.runAndMeasure(spark.sql(query).write.csv(s"/tmp/${queryId}_${scala.util.Random.nextLong()}.csv")))._2
    }

    val metrics = stageMetrics.createStageMetricsDF()
    val aggMetrics = metrics
      .drop(columnsToDrop: _*)
      .groupBy()
      .sum()
    val testMetaDF = spark.createDataFrame(Array(TestMetaRecord(
      queryId,
      algoName,
      java.sql.Timestamp.valueOf(LocalDateTime.now()),
      defaultToNone(spark.sparkContext.getConf.getInt("spark.executor.instances",-1)),
      defaultToNone(spark.sparkContext.getConf.getInt("spark.executor.cores",spark
        .sparkContext
        .master
        .replace("local","")
        .replace("[","")
        .replace("]","") match {
          case "*" => -1
          case r:String  =>r.toInt
      })),
      defaultToNone(spark.sparkContext.getConf.getSizeAsGb("spark.executor.memory","0").toInt),
      defaultToNone(spark.sparkContext.getConf.getSizeAsGb("spark.driver.memory","0").toInt),
      tables,
      arraysCount,
      executionTime
    )))
    spark.experimental.extraStrategies =  Nil
    val result = testMetaDF.crossJoin(aggMetrics)
    result.createOrReplaceTempView("metrics_record")
    spark.sql(
      s"""
        |INSERT INTO ${metricsTableName} SELECT * FROM metrics_record
      """.stripMargin)

  }
  def runAndCollectMetricsADAM(queryId:String,algoName:String,tables:Array[String],targetsPath:String,readsPath:String) = {

    val arraysCount = new Array[Long](tables.size)
    for(i<- 0 to arraysCount.size - 1 ){
      arraysCount(i) =
        spark.sql(s"SELECT count(*) FROM ${tables(i)}")
          .first()
          .getLong(0)
    }
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)

    val targets = spark.sparkContext.loadFeatures(targetsPath)
    val reads = spark.sparkContext.loadAlignments(readsPath)
    val res = targets.broadcastRegionJoin(reads)
    val executionTime = {
      time(stageMetrics.runAndMeasure(res.rdd.count()))._2
    }

    val metrics = stageMetrics.createStageMetricsDF()
    val aggMetrics = metrics
      .drop(columnsToDrop: _*)
      .groupBy()
      .sum()
    val testMetaDF = spark.createDataFrame(Array(TestMetaRecord(
      queryId,
      algoName,
      java.sql.Timestamp.valueOf(LocalDateTime.now()),
      defaultToNone(spark.sparkContext.getConf.getInt("spark.executor.instances",-1)),
      defaultToNone(spark.sparkContext.getConf.getInt("spark.executor.cores",-1)),
      defaultToNone(spark.sparkContext.getConf.getSizeAsGb("spark.executor.memory","0").toInt),
      defaultToNone(spark.sparkContext.getConf.getSizeAsGb("spark.driver.memory","0").toInt),
      tables,
      arraysCount,
      executionTime
    )))
    spark.experimental.extraStrategies =  Nil
    val result = testMetaDF.crossJoin(aggMetrics)
    result.createOrReplaceTempView("metrics_record")
    spark.sql(
      s"""
         |INSERT INTO ${metricsTableName} SELECT * FROM metrics_record
      """.stripMargin)

  }

  private def defaultToNone(p: Int) = if (p>0) Some(p) else None
}
