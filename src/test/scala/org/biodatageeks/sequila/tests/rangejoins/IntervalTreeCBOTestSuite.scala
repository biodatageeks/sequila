//package org.biodatageeks.sequila.tests.rangejoins
//
//import java.io.{OutputStreamWriter, PrintWriter}
//
//import com.holdenkarau.spark.testing.DataFrameSuiteBase
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.types.{
//  IntegerType,
//  StringType,
//  StructField,
//  StructType
//}
//import org.bdgenomics.utils.instrumentation.{
//  Metrics,
//  MetricsListener,
//  RecordedMetrics
//}
//import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
//import org.scalatest.{BeforeAndAfter, FunSuite}
//
//import scala.util.Random
//
//class IntervalTreeCBOTestSuite
//    extends FunSuite
//    with DataFrameSuiteBase
//    with BeforeAndAfter {
//
//  val schema = StructType(
//    Seq(StructField("chr", StringType),
//        StructField("start", IntegerType),
//        StructField("end", IntegerType)))
//  val metricsListener = new MetricsListener(new RecordedMetrics())
//  val writer = new PrintWriter(new OutputStreamWriter(System.out))
//
//  before {
//    val spark = SparkSession
//      .builder()
//      .enableHiveSupport()
//      .getOrCreate()
//
//    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
//      spark) :: Nil
//    spark.sqlContext.setConf("minOverlap", "1")
//    spark.sqlContext.setConf("maxGap", "0")
//
//    Metrics.initialize(sc)
//    val rdd = sc.parallelize(1 to 10000).map(k => Row("1", k, k))
//    val ds1 = spark.sqlContext.createDataFrame(rdd, schema)
//    ds1.createOrReplaceTempView("s1")
//
//    sc.addSparkListener(metricsListener)
//    spark.sqlContext.sql("""
//        |CREATE TABLE t1 AS SELECT * FROM s1
//      """.stripMargin)
//
//    spark.sqlContext.sql("""
//        |CREATE TABLE t2 AS SELECT * FROM s1
//      """.stripMargin)
//
//    spark.sqlContext
//      .sql("""
//        |SHOW TABLES
//      """.stripMargin)
//      .show()
//
//    spark.sqlContext.sql("""
//        |ANALYZE TABLE t1 COMPUTE STATISTICS
//      """.stripMargin)
//    spark.sqlContext.sql("""
//        |ANALYZE TABLE t2 COMPUTE STATISTICS
//      """.stripMargin)
//  }
//
//  test("CBO using stats using JoinWithRowBroadcast algorithm") {
//
//    spark.sqlContext.sql("DESC EXTENDED t1").show
//    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize",
//                             (10 * 1024 * 1024).toString)
//    val sqlQuery =
//      "SELECT * FROM t2 JOIN t1 ON (t1.chr=t2.chr AND t1.end>=t2.start AND t1.start<=t2.end )"
//    assert(spark.sqlContext.sql(sqlQuery).count === 10000)
//  }
//
//  test("CBO using stats using TwoPhaseJoin algorithm") {
//
//    spark.sqlContext.sql("DESC EXTENDED t1").show
//    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize",
//                             (1024 * 1024).toString)
//    val sqlQuery =
//      "SELECT * FROM t2 JOIN t1 ON (t1.chr=t2.chr AND t1.end>=t2.start AND t1.start<=t2.end )"
//    assert(spark.sqlContext.sql(sqlQuery).count === 10000)
//  }
//
//  after {
//
//    val schema = StructType(
//      Seq(
//        StructField("chr", StringType),
//        StructField("start", IntegerType),
//        StructField("end", IntegerType),
//        StructField("text_1", StringType),
//        StructField("text_2", StringType),
//        StructField("text_3", StringType),
//        StructField("text_4", StringType),
//        StructField("text_5", StringType)
//      ))
//
//    val rdd = sc
//      .parallelize(1L to 1000000L)
//      .map(k =>
//        Row(
//          s"${1 + (math.random * 20).toInt}",
//          k.toInt,
//          (k + k * math.random * (100)).toInt,
//          Random.nextString(10),
//          Random.nextString(15),
//          Random.nextString(5),
//          Random.nextString(10),
//          Random.nextString(3)
//      ))
//
//    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
//    writer.flush()
//    spark.sqlContext.sql("""
//        |DROP TABLE t1
//      """.stripMargin)
//    spark.sqlContext.sql("""
//        |DROP TABLE t2
//      """.stripMargin)
//    spark.sqlContext
//      .sql("""
//        |SHOW TABLES
//      """.stripMargin)
//      .show()
//    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
//    writer.flush()
//    Metrics.stopRecording()
//  }
//
//}
