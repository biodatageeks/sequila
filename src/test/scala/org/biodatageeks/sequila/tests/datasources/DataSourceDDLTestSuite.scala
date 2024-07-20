package org.biodatageeks.sequila.tests.datasources

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SequilaSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.scalatest.funsuite.AnyFunSuite

class DataSourceDDLTestSuite
      extends AnyFunSuite
        with DataFrameSuiteBase
      with SharedSparkContext {

  val bamPath: String = getClass.getResource("/NA12878.slice.bam").getPath
  val tableNameBAM = "reads"

  test("Test DDL in SequilaSession"){

    val ss = new SequilaSession(spark)
    ss.sql(s"DROP TABLE IF EXISTS $tableNameBAM")
    ss.sql(s"""
                 |CREATE TABLE $tableNameBAM
                 |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
                 |OPTIONS(path "$bamPath")
                 |
      """.stripMargin)

  }

}
