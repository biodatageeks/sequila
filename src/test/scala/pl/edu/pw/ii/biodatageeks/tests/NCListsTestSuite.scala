package pl.edu.pw.ii.biodatageeks.tests

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import genApp.IntervalTreeJoinStrategy
import ncl.NCListsJoinStrategy
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FunSuite}

class NCListsTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter{
  val schema1 = StructType(Seq(StructField("start1", LongType), StructField("end1", LongType)))
  val schema2 = StructType(Seq(StructField("start2", LongType), StructField("end2", LongType)))
  val schema3 = StructType(Seq(StructField("start1", LongType), StructField("end1", LongType), StructField("start2", LongType), StructField("end2", LongType)))
  val schema4 = StructType(Seq(StructField("start1", LongType)))
  val schema5 = StructType(Seq(StructField("start2", LongType), StructField("end2", LongType), StructField("start1", LongType), StructField("end1", LongType)))

  before {
    spark.experimental.extraStrategies = new NCListsJoinStrategy(spark) :: Nil
    var rdd1 = sc.parallelize(Seq(
      (100L, 199L),
      (200L, 299L),
      (400L, 600L),
      (10000L, 20000L),
      (22100L, 22100L)))
      .map(i => Row(i._1, i._2))
    var rdd2 = sc.parallelize(Seq(
      (150L, 250L),
      (199L,300L),
      (300L, 500L),
      (500L, 700L),
      (22000L, 22300L),
      (15000L, 15000L)))
      .map(i => Row(i._1, i._2))


    var ds1 = sqlContext.createDataFrame(rdd1, schema1)
    ds1.createOrReplaceTempView("s1")
    var ds2 = sqlContext.createDataFrame(rdd2, schema2)
    ds2.createOrReplaceTempView("s2")
  }

  test("range join select one field - left larger") {
    val sqlQuery = "select start1 from s1 JOIN s2 on (end1>=start2 and start1<=end2 )"
    println(sqlQuery)
    sqlContext.sql(sqlQuery).explain
    sqlContext.sql(sqlQuery).orderBy("start1").show
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(100L) ::
          Row(100L) ::
          Row(200L) ::
          Row(200L) ::
          Row(400L) ::
          Row(400L) ::
          Row(10000L) ::
          Row(22100L) ::
          Nil),schema4).orderBy("start1"),
      sqlContext.sql(sqlQuery).orderBy("start1"))
  }

  test("range join select * - left larger") {
    val sqlQuery = "select * from s1 JOIN s2 on (end1>=start2 and start1<=end2 )"
    println(sqlQuery)
    sqlContext.sql(sqlQuery).explain
    sqlContext.sql(sqlQuery).orderBy("start1").show
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(100L, 199L, 150L, 250L) ::
          Row(100L, 199L, 199L, 300L) ::
          Row(200L, 299L, 150L, 250L) ::
          Row(200L, 299L, 199L, 300L) ::
          Row(400L, 600L, 300L, 500L) ::
          Row(400L, 600L, 500L, 700L) ::
          Row(10000L, 20000L, 15000L, 15000L) ::
          Row(22100L, 22100L, 22000L, 22300L) ::
          Nil),schema3).orderBy("start1"),
      sqlContext.sql(sqlQuery).orderBy("start1"))
  }

  test("range join select one field - right larger") {
    val sqlQuery = "select start1 from s2 JOIN s1 on (end2>=start1 and start2<=end1 )"
    println(sqlQuery)
    sqlContext.sql(sqlQuery).explain
    sqlContext.sql(sqlQuery).orderBy("start1").show
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(100L) ::
          Row(100L) ::
          Row(200L) ::
          Row(200L) ::
          Row(400L) ::
          Row(400L) ::
          Row(10000L) ::
          Row(22100L) ::
          Nil),schema4).orderBy("start1"),
      sqlContext.sql(sqlQuery).orderBy("start1"))
  }

  test("range join select * - right larger") {
    val sqlQuery = "select * from s2 JOIN s1 on (end2>=start1 and start2<=end1 )"
    println(sqlQuery)
    sqlContext.sql(sqlQuery).explain
    sqlContext.sql(sqlQuery).orderBy("start1").show
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(150L, 250L, 100L, 199L) ::
          Row(199L, 300L, 100L, 199L) ::
          Row(150L, 250L, 200L, 299L) ::
          Row(199L, 300L, 200L, 299L) ::
          Row(300L, 500L, 400L, 600L) ::
          Row(500L, 700L, 400L, 600L) ::
          Row(15000L, 15000L, 10000L, 20000L) ::
          Row(22000L, 22300L, 22100L, 22100L) ::
          Nil),schema5).orderBy("start1"),
      sqlContext.sql(sqlQuery).orderBy("start1"))
  }
}
