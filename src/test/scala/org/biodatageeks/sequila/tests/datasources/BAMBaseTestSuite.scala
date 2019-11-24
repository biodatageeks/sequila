package org.biodatageeks.sequila.tests.datasources

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class BAMBaseTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  val bamPath: String = getClass.getResource("/NA12878.slice.bam").getPath
  val tableNameBAM = "reads"

  before {
    spark.sql(s"DROP TABLE IF EXISTS $tableNameBAM")
    spark.sql(s"""
         |CREATE TABLE $tableNameBAM
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)
  }

  test("Repartitioning") {
    spark.sqlContext.setConf(InternalParams.InputSplitSize, "100000")
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val a = ss.sql(s"""
        |SELECT * FROM $tableNameBAM
      """.stripMargin)
    println(s"""Partitions number: ${a.rdd.partitions.length}""")

  }

}
