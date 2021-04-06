package org.biodatageeks.sequila.tests.datasources

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.rangejoins.genApp.IntervalTreeJoinStrategy
import org.biodatageeks.sequila.utils.{Columns, InternalParams}
import org.scalatest.{BeforeAndAfter, FunSuite}

class ADAMBenchmarkTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  def time[A](f: => A): A = {
    val s = System.nanoTime
    val ret = f
    println("time: " + (System.nanoTime - s) / 1e9 + " seconds")
    ret
  }
  val query: String = s"""
     | SELECT * FROM snp JOIN ref
     | ON (ref.${Columns.CONTIG} = snp.${Columns.CONTIG}
     | AND
     | CAST(snp.${Columns.END} AS INTEGER) >= CAST(ref.${Columns.START} AS INTEGER)
     | AND
     | CAST(snp.${Columns.START} AS INTEGER) <= CAST(ref.${Columns.END} AS INTEGER)
     |)
     """.stripMargin


  before {
    System.setSecurityManager(null)
    //spark.sparkContext.setLogLevel("INFO")
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil
    spark.sqlContext.setConf(InternalParams.maxBroadCastSize,
                             (5 * 1024 * 1024).toString)

    val tableRef = "ref"
    val refPath = getClass.getResource("/refFlat.adam").getPath
    spark.sql(s"DROP TABLE IF EXISTS $tableRef")
    spark.sql(s"""
                 |CREATE TABLE $tableRef
                 |USING org.biodatageeks.sequila.datasources.ADAM.ADAMDataSource
                 |OPTIONS(path "$refPath")
                 |
      """.stripMargin)

    val tableSnp = "snp"
    val snpPath = getClass.getResource("/snp150Flagged.adam").getPath
    spark.sql(s"DROP TABLE IF EXISTS $tableSnp")
    spark.sql(s"""
                 |CREATE TABLE $tableSnp
                 |USING org.biodatageeks.sequila.datasources.ADAM.ADAMDataSource
                 |OPTIONS(path "$snpPath")
                 |
      """.stripMargin)

    spark.sql(s"""select * from $tableSnp limit 1""").show()

  }

  test("Join using bgd-spark-granges - broadcast") {

    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)

    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil
    time(
      assert(stageMetrics
        .runAndMeasure(spark.sqlContext.sql(query).count) === 616404L))

  }

  test("Join using bgd-spark-granges - twophase") {
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil
    sqlContext.setConf(InternalParams.maxBroadCastSize,
                       (1024 * 1024).toString)
    time(
      assert(stageMetrics
        .runAndMeasure(spark.sqlContext.sql(query).count) === 616404L))
    val a = stageMetrics.createStageMetricsDF()
    val b = a
      .drop("jobId", "stageId", "name", "submissionTime", "completionTime")
      .groupBy()
      .sum()

    b.select("sum(executorRunTime)",
              "sum(executorCpuTime)",
              "sum(shuffleTotalBytesRead)",
              "sum(shuffleBytesWritten)")
      .show(100, truncate = false)
  }


  test("Join using builtin spark algo") {

    spark.experimental.extraStrategies = Nil
    time(assert(spark.sqlContext.sql(query).count === 616404L))
  }

  test("Join using builtin genapp") {

    spark.experimental.extraStrategies = new IntervalTreeJoinStrategy(spark) :: Nil
    time(assert(spark.sqlContext.sql(query).count === 616404L))
  }

}
