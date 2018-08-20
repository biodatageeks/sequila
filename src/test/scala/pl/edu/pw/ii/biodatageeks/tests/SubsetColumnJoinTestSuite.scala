package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim

import scala.util.Random

class SubsetColumnJoinTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter{


  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))

  before {
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    val schema = StructType(Seq(StructField("sample", StringType),StructField("chr", StringType),StructField("start", IntegerType), StructField("end", IntegerType),StructField("coverage", IntegerType)))
    val rdd = sc.parallelize(1L to 1000000L).map(k=>Row(s"sample${1+(math.random *100).toInt}",s"${1+(math.random *20).toInt}",k.toInt,(k+k*math.random * (100)).toInt,(math.random * (30) + 10).toInt))
    val ds1 = spark.sqlContext.createDataFrame(rdd, schema)
    ds1.createOrReplaceTempView("sample_interval_temp")

    val schema2 = StructType(Seq(StructField("chr", StringType),StructField("start", IntegerType), StructField("end", IntegerType),StructField("text_1", StringType),StructField("text_2", StringType),StructField("text_3", StringType),StructField("text_4", StringType),StructField("text_5", StringType)))
    val rdd2 = sc.parallelize(1L to 100L).map(k=>Row(s"${1+(math.random *20).toInt}",k.toInt,(k+k*math.random * (100)).toInt,Random.alphanumeric.take(10).mkString,Random.alphanumeric.take(15).mkString,Random.alphanumeric.take(5).mkString,Random.alphanumeric.take(10).mkString,Random.alphanumeric.take(3).mkString))
    val ds2 = spark.sqlContext.createDataFrame(rdd2, schema2)
    ds2.createOrReplaceTempView("annotation_temp")

    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)

  }

  test ("Select subset of columms"){

    val sqlQuery =
      """
        |SELECT s2.chr,text_1,s2.start,s2.end,s1.start,s1.end FROM
        |sample_interval_temp s2
        |JOIN
        |annotation_temp s1
        |ON (s1.end>=s2.start and s1.start<=s2.end )
        |WHERE s2.start=2
        |LIMIT 5
      """.stripMargin
    spark.sqlContext
      .sql(sqlQuery)
      .explain(false)

    spark.sqlContext
      .sql(sqlQuery)
      .show


  }

  after{

    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    Metrics.stopRecording()

  }

}
