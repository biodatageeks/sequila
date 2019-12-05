package org.biodatageeks.sequila.tests.base

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class BEDBaseTestSuite
    extends
      FunSuite
    with DataFrameSuiteBase
    with SharedSparkContext with BeforeAndAfter{

  val bedPath: String = getClass.getResource("/bed/test.bed").getPath
  val tableNameBED = "targets"

  val bedSimplePath: String = getClass.getResource("/bed/simple.bed").getPath
  val tableNameSimpleBED = "simple_targets"


  before{
    spark.sql(s"DROP TABLE IF EXISTS $tableNameBED")
    spark.sql(s"""
         |CREATE TABLE $tableNameBED
         |USING org.biodatageeks.sequila.datasources.BED.BEDDataSource
         |OPTIONS(path "$bedPath")
         |
      """.stripMargin)
    spark.sql(s"DROP TABLE IF EXISTS $tableNameSimpleBED")
    spark.sql(s"""
                 |CREATE TABLE $tableNameSimpleBED
                 |USING org.biodatageeks.sequila.datasources.BED.BEDDataSource
                 |OPTIONS(path "$bedSimplePath")
                 |
      """.stripMargin)

  }

  def after = {

    spark.sql(s"DROP TABLE IF EXISTS $tableNameBED")
    spark.sql(s"DROP TABLE IF EXISTS $tableNameSimpleBED")

  }


}
