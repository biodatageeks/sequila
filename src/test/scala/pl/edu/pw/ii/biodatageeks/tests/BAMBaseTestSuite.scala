package pl.edu.pw.ii.biodatageeks.tests

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.utils.{BDGInternalParams, SequilaRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class BAMBaseTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{

  val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
  //val bamPath = "/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam"

  val tableNameBAM = "reads"

  before{
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)
  }
//
//  test("Repartitioning"){
//    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize,"100000")
//    val  ss = SequilaSession(spark)
//    SequilaRegister.register(ss)
//
//
//    val a = ss.sql(
//      s"""
//        |SELECT * FROM ${tableNameBAM}
//      """.stripMargin)
//    println(s"""Partitions number: ${a
//      .rdd
//      .partitions.length}""" )
//
//  }


}
