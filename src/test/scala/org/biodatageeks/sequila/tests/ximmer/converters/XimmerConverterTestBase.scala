package org.biodatageeks.sequila.tests.ximmer.converters

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.biodatageeks.sequila.utils.InternalParams
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import scala.collection.mutable
import scala.reflect.io.Directory

class XimmerConverterTestBase extends FunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfter {

  val tempDir = "src/test/resources/ximmer/temp"
  val ximmerResourceDir = "src/test/resources/ximmer"
  val targetCountsResult: mutable.Map[String, (Array[Row], Long)] = mutable.SortedMap[String, (DataFrame, DataFrame)]()

  val schema = StructType(
    Seq(StructField("chr", StringType),
      StructField("start", StringType),
      StructField("end", StringType),
      StructField("codex_cov", LongType),
      StructField("cnmops_cov", LongType),
      StructField("ed_cov", LongType),
      StructField("conifer_cov", LongType)))

  before {
    import spark.implicits._
    val directory = new Directory(new File(tempDir))
    directory.createDirectory()

    val df1 = spark.read
      .option("header", "false")
      .schema(schema)
      .csv(ximmerResourceDir + "/converter_tests_input/target_counts_XI001.csv")
    val df2 = spark.read
      .option("header", "false")
      .schema(schema)
      .csv(ximmerResourceDir + "/converter_tests_input/target_counts_XI002.csv")
    targetCountsResult += ("XI001" -> (df1.collect(), 735229))
    targetCountsResult += ("XI002" -> (df2.collect(), 930845))

    spark.conf.set(InternalParams.saveAsSparkFormat, "false")
  }

  def after: Unit = {
    val directory = new Directory(new File(tempDir))
    directory.deleteRecursively()
  }
}
