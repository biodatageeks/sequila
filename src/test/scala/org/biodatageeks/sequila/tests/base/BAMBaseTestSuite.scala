package org.biodatageeks.sequila.tests.base

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.SequilaRegister
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

class BAMBaseTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with SharedSparkContext with BeforeAndAfter{

  val bamPath: String = getClass.getResource("/NA12878.slice.md.bam").getPath
  val tableNameBAM = "reads"

  before{
    spark.sql(s"DROP TABLE IF EXISTS $tableNameBAM")
    spark.sql(s"""
         |CREATE TABLE $tableNameBAM
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)

  }

  def after = {

    spark.sql(s"DROP TABLE IF EXISTS $tableNameBAM")

  }


}
