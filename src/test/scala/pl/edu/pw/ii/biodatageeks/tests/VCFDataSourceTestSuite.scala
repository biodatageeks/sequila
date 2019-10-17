package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.utils.BDGInternalParams
import org.scalatest.{BeforeAndAfter, FunSuite}

class VCFDataSourceTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{


  val vcfPath = getClass.getResource("/vcf/test.vcf").getPath
  val tableNameVCF = "variants"
  before{
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameVCF}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameVCF}
         |USING org.biodatageeks.datasources.VCF.VCFDataSource
         |OPTIONS(path "${vcfPath}")
         |
      """.stripMargin)

  }
  test("VCF - Row count VCFDataSource"){
    val query = s"SELECT * FROM ${tableNameVCF}"
    spark
      .sql(query)
      .printSchema()

    println(spark.sparkContext.hadoopConfiguration.get("hadoop.io.compression.codecs"))

    assert(spark
      .sql(query)
      .first()
      .getString(8) === "PASS")

    assert(spark.sql(query).count() === 21L)
  }




  after{
    spark.sql(s"DROP TABLE IF EXISTS  ${tableNameVCF}")
  }

}
