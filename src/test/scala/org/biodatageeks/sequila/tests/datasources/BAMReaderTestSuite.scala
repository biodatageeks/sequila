package org.biodatageeks.sequila.tests.datasources

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.{Columns, InternalParams}
import org.scalatest.{BeforeAndAfter, FunSuite}

class BAMReaderTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  val bamPath: String = getClass.getResource("/NA12878.slice.bam").getPath

  val tableNameBAM = "reads"

  before {
    spark.sql(s"DROP TABLE IF EXISTS $tableNameBAM")
    spark.sql(
      s"""
         |CREATE TABLE $tableNameBAM
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)
  }

  test("Test point query predicate pushdown") {
    val ss = SequilaSession(spark)
    ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown", "false")

    val query =
      s"""
         |SELECT * FROM reads WHERE ${Columns.CONTIG}='1' AND ${Columns.START}=20138
                     """.stripMargin
    val withoutPPDF = ss.sql(query).collect()

    ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown", "true")
    val withPPDF = ss.sql(query)
    assertDataFrameEquals(
      ss.createDataFrame(ss.sparkContext.parallelize(withoutPPDF),
        withPPDF.schema),
      withPPDF)
    spark.time {
      ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown", "false")
      ss.sql(query).count
    }

  }

  test("Test interval query predicate pushdown") {
    val ss = SequilaSession(spark)
    ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown", "false")
    val query =
      s"""
         |SELECT * FROM reads WHERE ${Columns.CONTIG}='1' AND ${Columns.START} >= 1996 AND ${Columns.END} <= 2071
                 """.stripMargin
    val withoutPPDF = ss.sql(query).collect()

    ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown", "true")
    val withPPDF = ss.sql(query)
    assertDataFrameEquals(
      ss.createDataFrame(ss.sparkContext.parallelize(withoutPPDF),
        withPPDF.schema),
      withPPDF)
    spark.time {
      ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown", "false")
      ss.sql(query).show(50)
    }

  }

  test("Read BAM wide record") {
    val ss = SequilaSession(spark)
    val df = ss.sql(
      s"""
         |SELECT * FROM $tableNameBAM
      """.stripMargin)
    df.printSchema()
    df.show(1, false)

  }


  test("Repartitioning") {
    spark.sqlContext.setConf(InternalParams.InputSplitSize, "100000")
    val ss = SequilaSession(spark)

    val a = ss.sql(
      s"""
         |SELECT * FROM $tableNameBAM
      """.stripMargin)
    println(s"""Partitions number: ${a.rdd.partitions.length}""")

  }


  test("Simple select over a BAM table group by") {
    val ss = SequilaSession(spark)
    assert(
      ss.sql(
        s"SELECT ${Columns.SAMPLE},count(*) FROM $tableNameBAM group by ${Columns.SAMPLE}")
        .first()
        .getLong(1) === 3172)
  }

  test("Read BAM file with LIMIT") {
    val ss = SequilaSession(spark)
    ss.sparkContext.setLogLevel("INFO")
    val df = ss.sql(
      s"""
         |SELECT * FROM $tableNameBAM LIMIT 1
      """.stripMargin)
    df.show(1, false)

  }
}