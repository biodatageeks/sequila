package org.biodatageeks.sequila.tests.rangejoins

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}

import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.utils.Columns
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Random

class SubsetColumnJoinTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter {


  before {
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil
    val schema = StructType(
      Seq(
        StructField(s"${Columns.SAMPLE}", StringType),
        StructField(s"${Columns.CONTIG}", StringType),
        StructField(s"${Columns.START}", IntegerType),
        StructField(s"${Columns.END}", IntegerType),
        StructField(s"${Columns.COVERAGE}", IntegerType)
      ))
    val rdd = sc
      .parallelize(1L to 1000000L)
      .map(
        k =>
          Row(s"sample${1 + (math.random * 100).toInt}",
              s"${1 + (math.random * 20).toInt}",
              k.toInt,
              (k + k * math.random * 100).toInt,
              (math.random * 30 + 10).toInt))
    val ds1 = spark.sqlContext.createDataFrame(rdd, schema)
    ds1.createOrReplaceTempView("sample_interval_temp")

    val schema2 = StructType(
      Seq(
        StructField(s"${Columns.CONTIG}", StringType),
        StructField(s"${Columns.START}", IntegerType),
        StructField(s"${Columns.END}", IntegerType),
        StructField("text_1", StringType),
        StructField("text_2", StringType),
        StructField("text_3", StringType),
        StructField("text_4", StringType),
        StructField("text_5", StringType)
      ))
    val rdd2 = sc
      .parallelize(1L to 100L)
      .map(k =>
        Row(
          s"${1 + (math.random * 20).toInt}",
          k.toInt,
          (k + k * math.random * 100).toInt,
          Random.alphanumeric.take(10).mkString,
          Random.alphanumeric.take(15).mkString,
          Random.alphanumeric.take(5).mkString,
          Random.alphanumeric.take(10).mkString,
          Random.alphanumeric.take(3).mkString
      ))
    val ds2 = spark.sqlContext.createDataFrame(rdd2, schema2)
    ds2.createOrReplaceTempView("annotation_temp")


  }

  test("Select subset of columns") {

    val sqlQuery =
      s"""
        |SELECT s2.${Columns.CONTIG},text_1,s2.${Columns.START},s2.${Columns.END},s1.${Columns.START},s1.${Columns.END}
        |FROM sample_interval_temp s2 JOIN annotation_temp s1
        |ON (s1.${Columns.END}>=s2.${Columns.START} and s1.${Columns.START}<=s2.${Columns.END} )
        |WHERE s2.${Columns.START}=2
        |LIMIT 5
      """.stripMargin

    spark.sqlContext
      .sql(sqlQuery)
      .explain(false)

    spark.sqlContext
      .sql(sqlQuery)
      .show

  }


}
