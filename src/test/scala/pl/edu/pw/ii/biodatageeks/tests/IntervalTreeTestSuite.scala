package pl.edu.pw.ii.biodatageeks.tests

import com.holdenkarau.spark.testing.{DataFrameSuiteBase}
import genApp.IntervalTreeJoinStrategy
import org.apache.spark.sql.{Row}
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.types.{LongType, StructField, StructType}
class IntervalTreeTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter{

  val schema3 = StructType(Seq(StructField("start1", LongType), StructField("end1", LongType), StructField("start2", LongType), StructField("end2", LongType)))

  before {
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategy(spark) :: Nil
    val schema1 = StructType(Seq(StructField("start1", LongType), StructField("end1", LongType)))
    val schema2 = StructType(Seq(StructField("start2", LongType), StructField("end2", LongType)))
    var rdd1 = sc.parallelize(Seq((100L, 199L),
      (200L, 299L),
      (400L, 600L),
      (10000L, 20000L),
      (22100L, 22100L)))
      .map(i => Row(i._1, i._2))
    var rdd2 = sc.parallelize(Seq((150L, 250L),
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
  test("strict inequality range join") {
    val sqlQuery = "select * from s1 JOIN s2 on start1 <= end1 and start2 <= end2 and start1 <= start2 and start2 <= end1"
    sqlContext.sql(sqlQuery).explain
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(22100L,22100L,22000L, 22300L) ::
        Row(100L, 199L, 150L, 250L) ::
          Row(200L, 299L, 150L, 250L) ::
          Row(10000L, 20000L,15000L, 15000L) ::
          Row(400L, 600L, 300L, 500L) ::
          Row(400L, 600L, 500L, 700L) :: Nil),schema3),
      sqlContext.sql(sqlQuery))
  }

  test("non strict inequality range join") {
    val sqlQuery = "select * from s1 JOIN s2 on start1 < end1 and start2 < end2 and start1 < start2 and start2 < end1"
    sqlContext.sql(sqlQuery).explain
    assertDataFrameEquals(
      sqlContext.createDataFrame(sc.parallelize(
        Row(100L, 199L, 150L, 250L) ::
          Row(200L, 299L, 150L, 250L) ::
          Row(400L, 600L, 300L, 500L) ::
          Row(400L, 600L, 500L, 700L) :: Nil),schema3),
      sqlContext.sql(sqlQuery))
  }
}
