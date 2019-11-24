package org.biodatageeks.sequila.tests.datasources

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class VCFDataSourceTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  val vcfPath: String = getClass.getResource("/vcf/test.vcf").getPath
  val tableNameVCF = "variants"
  before {
    spark.sql(s"DROP TABLE IF EXISTS $tableNameVCF")
    spark.sql(s"""
         |CREATE TABLE $tableNameVCF
         |USING org.biodatageeks.sequila.datasources.VCF.VCFDataSource
         |OPTIONS(path "$vcfPath")
         |
      """.stripMargin)

  }
  test("VCF - Row count VCFDataSource") {
    val query = s"SELECT * FROM $tableNameVCF"
    spark
      .sql(query)
      .printSchema()

    println(
      spark.sparkContext.hadoopConfiguration
        .get("hadoop.io.compression.codecs"))

    assert(
      spark
        .sql(query)
        .first()
        .getString(8) === "PASS")

    assert(spark.sql(query).count() === 21L)
  }

  after {
    spark.sql(s"DROP TABLE IF EXISTS  $tableNameVCF")
  }

}
