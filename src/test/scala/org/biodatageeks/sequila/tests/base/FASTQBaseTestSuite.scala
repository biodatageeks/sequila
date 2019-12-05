package org.biodatageeks.sequila.tests.base

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class FASTQBaseTestSuite
    extends
      FunSuite
    with DataFrameSuiteBase
    with SharedSparkContext with BeforeAndAfter{

  val fastqPath: String = getClass.getResource("/fastq/NA12988.fastq").getPath
  val tableNameFASTQ = "fragmets"


  before{
    spark.sql(s"DROP TABLE IF EXISTS $tableNameFASTQ")
    spark.sql(s"""
         |CREATE TABLE $tableNameFASTQ
         |USING org.biodatageeks.sequila.datasources.FASTQ.FASTQDataSource
         |OPTIONS(path "$fastqPath")
         |
      """.stripMargin)

  }

  def after = {

    spark.sql(s"DROP TABLE IF EXISTS $tableNameFASTQ")

  }


}
