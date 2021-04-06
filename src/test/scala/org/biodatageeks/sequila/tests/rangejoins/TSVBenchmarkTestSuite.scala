package org.biodatageeks.sequila.tests.rangejoins

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.rangejoins.genApp.IntervalTreeJoinStrategy
import org.biodatageeks.sequila.utils.{Columns, InternalParams}
import org.scalatest.{BeforeAndAfter, FunSuite}

class TSVBenchmarkTestSuite
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

  val schema = StructType(
    Seq(StructField(s"${Columns.CONTIG}", StringType),
        StructField(s"${Columns.START}", IntegerType),
        StructField(s"${Columns.END}", IntegerType)))

  val query: String = s"""
      | SELECT * FROM snp JOIN ref
      | ON (ref.${Columns.CONTIG}=snp.${Columns.CONTIG}
      | AND
      | CAST(snp.${Columns.END} AS INTEGER)>=CAST(ref.${Columns.START} AS INTEGER)
      | AND
      | CAST(snp.${Columns.START} AS INTEGER)<=CAST(ref.${Columns.END} AS INTEGER)
)""".stripMargin

  before {
    System.setSecurityManager(null)
    //spark.sparkContext.setLogLevel("INFO")
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil
    sqlContext.setConf(InternalParams.maxBroadCastSize,
                       (100 * 1024 * 1024).toString)
    val rdd1 = sc
      .textFile(getClass.getResource("/refFlat.txt.bz2").getPath)
      .map(r => r.split('\t'))
      .map(
        r =>
          Row(
            r(2).toString,
            r(4).toInt,
            r(5).toInt
        ))
    val ref = spark
      .createDataFrame(rdd1, schema)
    ref.cache().count
    ref.createOrReplaceTempView("ref")

    val rdd2 = sc
      .textFile(getClass.getResource("/snp150Flagged.txt.bz2").getPath)
      .map(r => r.split('\t'))
      .map(
        r =>
          Row(
            r(1).toString,
            r(2).toInt,
            r(3).toInt
        ))
    val snp = spark
      .createDataFrame(rdd2, schema)
    snp.cache().count
    snp.createOrReplaceTempView("snp")

  }

  test("Join using bgd-spark-granges - broadcast") {

    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil
    time(assert(spark.sqlContext.sql(query).count === 616404L))
  }

  test("Join using bgd-spark-granges - twophase") {

    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil
    sqlContext.setConf(InternalParams.maxBroadCastSize,
                       (1024 * 1024).toString)
    time(assert(spark.sqlContext.sql(query).count === 616404L))
  }


  test("Join using builtin spark algo") {

    spark.experimental.extraStrategies = Nil
    time(assert(spark.sqlContext.sql(query).count === 616404L))
  }

  test("Join using builtin genap") {

    spark.experimental.extraStrategies = new IntervalTreeJoinStrategy(spark) :: Nil
    time(assert(spark.sqlContext.sql(query).count === 616404L))
  }

}
