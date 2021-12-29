package org.biodatageeks.sequila.tests.rangejoins

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.sequila.rangejoins.methods.chromosweep.ChromoSweepJoinStrategy
import org.scalatest.{BeforeAndAfter, FunSuite}

class ChromoSweepTest
  extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter {
  val oneFieldSchema = StructType(
    Seq(StructField("start", IntegerType)))
  val twoFieldSchema = StructType(
    Seq(StructField("start", IntegerType),
      StructField("end", IntegerType)))

  val resultSchema = StructType(Seq(
    StructField("start1", IntegerType),
    StructField("end1", IntegerType),
    StructField("rval1", StringType),
    StructField("start2", IntegerType),
    StructField("end2", IntegerType),
    StructField("rval2", StringType)))

  before {
    spark.experimental.extraStrategies = new ChromoSweepJoinStrategy(spark) :: Nil

    val rdd1 = sc
      .parallelize(Seq((100, 190), (200, 290), (400, 600), (10000, 20000), (22100, 22100)))
      .map(i => Row(i._1.toInt, i._2.toInt))
    val rdd2 = sc
      .parallelize(Seq((150, 180), (190, 300), (310, 500), (510, 700), (22000, 22300), (15000, 15000)))
      .map(i => Row(i._1.toInt, i._2.toInt))

    val ds1 = sqlContext.createDataFrame(rdd1, twoFieldSchema)
    ds1.createOrReplaceTempView("s1")
    val ds2 = sqlContext.createDataFrame(rdd2, twoFieldSchema)
    ds2.createOrReplaceTempView("s2")
  }

  test("simple functionality test") {
    val query = "select s1.start from s1 join s2 on (s1.end >= s2.start and s1.start <= s2.end)"
    assertDataFrameEquals(
      sqlContext
        .createDataFrame(
          sc.parallelize(Row(100) :: Row(100) :: Row(200) :: Row(400) :: Row(400) :: Row(10000) :: Row(22100) :: Nil),
          oneFieldSchema)
        .orderBy("start"),
      sqlContext.sql(query).orderBy("s1.start")
    )
  }

  // tests on bigger datasets

  test("test denser queries") {
    testGeneratedData("generated/denser-queries/queries/", "generated/denser-queries/labels/", "generated/denser-queries/results")
  }

  test("test denser labels") {
    testGeneratedData("generated/denser-labels/queries/", "generated/denser-labels/labels/", "generated/denser-labels/results")
  }


  private def testGeneratedData(queryPath: String, labelPath: String, resultPath: String) = {
    import spark.implicits._
    // workaround to avoid serialization error for CSVParser
    val ds3 = spark.sparkContext.textFile(queryPath).map(_.split(",")).map(e => (e(0).toInt, e(1).toInt, e(2))).toDF("start1", "end1", "val1")
    ds3.createOrReplaceTempView("dq1")
    val ds4 = spark.sparkContext.textFile(labelPath).map(_.split(",")).map(e => (e(0).toInt, e(1).toInt, e(2))).toDF("start2", "end2", "val2")
    ds4.createOrReplaceTempView("dq2")
    val expected = spark.read.schema(resultSchema).csv(resultPath)
    val sqlQuery1 = "select * from dq1 join dq2 on (end1 >= start2 and start1 <= end2)"
    val result = sqlContext.sql(sqlQuery1)
    val joined = result.join(expected, Seq("start1", "end1", "start2", "end2"))
    assertTrue(joined.where("(dq2.val2 == null and rval2 != null) or (dq2.val2 != null and rval2 == null)").count() == 0)
    assertTrue(joined.where("(dq2.val2 != null and rval2 != null) and dq2.val2 != rval2").count() == 0)
  }

}