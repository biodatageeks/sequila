package org.biodatageeks.sequila.tests.rangejoins

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}

import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.scalatest.{BeforeAndAfter, FunSuite}

class JoinOrderTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  val schema = StructType(
    Seq(StructField("chr", StringType),
        StructField("start", IntegerType),
        StructField("end", IntegerType)))
  before {
    System.setSecurityManager(null)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil

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
    val ref = spark.createDataFrame(rdd1, schema)
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
    snp.createOrReplaceTempView("snp")
  }

  test("Join order - broadcasting snp table") {
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder",
                             "true")
    val query =
      s"""
         |SELECT snp.*,ref.* FROM ref JOIN snp
         |ON (ref.chr=snp.chr AND snp.end>=ref.start AND snp.start<=ref.end)
       """.stripMargin

    assert(spark.sql(query).count === 616404L)

  }

  test("Join order - broadcasting ref table") {
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder",
                             "true")
    val query =
      s"""
         |SELECT snp.*,ref.* FROM snp JOIN ref
         |ON (ref.chr=snp.chr AND snp.end>=ref.start AND snp.start<=ref.end)
       """.stripMargin
    assert(spark.sql(query).count === 616404L)

  }
}
