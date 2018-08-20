package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.rangejoins.genApp.IntervalTreeJoinStrategy
import org.scalatest.{BeforeAndAfter, FunSuite}

class IntervalTreeTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter{
  val schema1 = StructType(Seq(StructField("start1",IntegerType ), StructField("end1", IntegerType)))
  val schema2 = StructType(Seq(StructField("start2", IntegerType), StructField("end2", IntegerType)))
  val schema3 = StructType(Seq(StructField("chr1",StringType ),StructField("start1",IntegerType ), StructField("end1", IntegerType)))
  val schema4 = StructType(Seq(StructField("chr2",StringType ),StructField("start2", IntegerType), StructField("end2", IntegerType)))

  val schema5 = StructType(Seq(StructField("start1", IntegerType), StructField("end1", IntegerType), StructField("start2", IntegerType), StructField("end2", IntegerType)))
  val schema6 = StructType(Seq(StructField("start1", IntegerType)))
  val schema7 = StructType(Seq(StructField("start2", IntegerType), StructField("end2", IntegerType), StructField("start1", IntegerType), StructField("end1", IntegerType)))

  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))

  before {
    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil

    Metrics.initialize(sc)

    sc.addSparkListener(metricsListener)

    var rdd1 = sc.parallelize(Seq(
      (100, 190),
      (200, 290),
      (400, 600),
      (10000, 20000),
      (22100, 22100)))
      .map(i => Row(i._1.toInt, i._2.toInt))
    var rdd2 = sc.parallelize(Seq(
      (150, 250),
      (190,300),
      (300, 500),
      (500, 700),
      (22000, 22300),
      (15000, 15000)))
      .map(i => Row(i._1.toInt, i._2.toInt))

    var rdd3 = sc.parallelize(Seq(
      ("1",100, 190),
      ("1",200, 290),
      ("1",400, 600),
      ("1",10000, 20000),
      ("1",22100, 22100),
      ("2",100, 190),
      ("2",200, 290),
      ("2",400, 600),
      ("2",10000, 20000),
      ("2",22100, 22100)
      )
      )
      .map(i => Row(i._1,i._2.toInt, i._3.toInt))
    var rdd4 = sc.parallelize(Seq(
      ("1",150, 250),
      ("1",190,300),
      ("1",300, 501),
      ("1",500, 700),
      ("1",22000, 22300),
      ("1",15000, 15000),
      ("2",150, 250),
      ("2",190,300),
      ("2",300, 500),
      ("2",500, 700),
      ("2",22000, 22300),
      ("2",15000, 15000)
    ))
      .map(i => Row(i._1,i._2.toInt, i._3.toInt))

    var ds1 = sqlContext.createDataFrame(rdd1, schema1)
    ds1.createOrReplaceTempView("s1")
    var ds2 = sqlContext.createDataFrame(rdd2, schema2)
    ds2.createOrReplaceTempView("s2")
    var ds3 = sqlContext.createDataFrame(rdd3, schema3)
    ds3.createOrReplaceTempView("s3")
    var ds4 = sqlContext.createDataFrame(rdd4, schema3)
    ds4.createOrReplaceTempView("s4")
  }

  test("range join chromosome") {

    spark.experimental.extraStrategies = Nil
    val sqlQuery = "select s3.chr1 as s3_chr,s3.start1 as s3_start1, s3.*,s4.* from s4 JOIN s3 on (s3.chr1=s4.chr1 and (s3.end1>=s4.start1 and s3.start1<=s4.end1 ))"
    val sqlQuery2 = "select s3.chr1 as s3_chr,s3.start1 as s3_start1, s3.*,s4.* from s3 JOIN s4 on (s3.chr1=s4.chr1 and (s3.end1>=s4.start1 and s3.start1<=s4.end1 ))"
    val df1 = spark.sql(sqlQuery).cache()
    df1.count
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    val df2 = spark.sql(sqlQuery2).cache()
    sqlContext.sql(sqlQuery2).explain
    df2.count
    println(sqlQuery)
    assertDataFrameEquals(df1.orderBy("s3_chr","s3_start1"),df2.orderBy("s3_chr","s3_start1"))
    sqlContext.sql(sqlQuery).explain
    sqlContext.sql(sqlQuery).orderBy("s3_start1").show
  }

  test("range join select one field - left larger") {
    val sqlQuery = "select start1 from s1 JOIN s2 on (end1>=start2 and start1<=end2 )"
    println(sqlQuery)
    sqlContext.sql(sqlQuery).explain(false)
    sqlContext.sql(sqlQuery).orderBy("start1").show
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(100) ::
          Row(100) ::
          Row(200) ::
          Row(200) ::
          Row(400) ::
          Row(400) ::
          Row(10000) ::
          Row(22100) ::
          Nil),schema6).orderBy("start1"),
      sqlContext.sql(sqlQuery).orderBy("start1"))
  }

  test("range join select * - left larger") {
    val sqlQuery = "select * from s1 JOIN s2 on (end1>=start2 and start1<=end2 )"
    println(sqlQuery)
    sqlContext.sql(sqlQuery).explain
    sqlContext.sql(sqlQuery).orderBy("start1").show
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(100, 190, 150, 250) ::
          Row(100, 190, 190, 300) ::
          Row(200, 290, 150, 250) ::
          Row(200, 290, 190, 300) ::
          Row(400, 600, 300, 500) ::
          Row(400, 600, 500, 700) ::
          Row(10000, 20000, 15000, 15000) ::
          Row(22100, 22100, 22000, 22300) ::
          Nil),schema5).orderBy("start1"),
      sqlContext.sql(sqlQuery).orderBy("start1"))
  }

  test("range join select one field - right larger") {
    val sqlQuery = "select start1 from s2 JOIN s1 on (end2>=start1 and start2<=end1 )"
    println(sqlQuery)
    sqlContext.sql(sqlQuery).explain
    sqlContext.sql(sqlQuery).orderBy("start1").show
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(100) ::
          Row(100) ::
          Row(200) ::
          Row(200) ::
          Row(400) ::
          Row(400) ::
          Row(10000) ::
          Row(22100) ::
          Nil),schema6).orderBy("start1"),
      sqlContext.sql(sqlQuery).orderBy("start1"))
  }

  test("range join select * - right larger") {
    val sqlQuery = "select * from s2 JOIN s1 on (end2>=start1 and start2<=end1 )"
    println(sqlQuery)
    sqlContext.sql(sqlQuery).explain
    sqlContext.sql(sqlQuery).orderBy("start1").show
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(150, 250, 100, 190) ::
          Row(190, 300, 100, 190) ::
          Row(150, 250, 200, 290) ::
          Row(190, 300, 200, 290) ::
          Row(300, 500, 400, 600) ::
          Row(500, 700, 400, 600) ::
          Row(15000, 15000, 10000, 20000) ::
          Row(22000, 22300, 22100, 22100) ::
          Nil),schema7).orderBy("start1"),
      sqlContext.sql(sqlQuery).orderBy("start1"))
  }

  after{

    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    Metrics.stopRecording()

  }
}
