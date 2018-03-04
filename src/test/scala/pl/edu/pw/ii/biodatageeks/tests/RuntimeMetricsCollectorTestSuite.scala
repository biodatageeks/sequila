package pl.edu.pw.ii.biodatageeks.tests

import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.rangejoins.common.metrics.MetricsCollector
import org.scalatest.{BeforeAndAfter, FunSuite}

class RuntimeMetricsCollectorTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext {


  val query = (
    s"""
       |SELECT * FROM snp JOIN ref
       |ON (ref.contigName=snp.contigName
       |AND
       |CAST(snp.end AS INTEGER)>=CAST(ref.start AS INTEGER)
       |AND
       |CAST(snp.start AS INTEGER)<=CAST(ref.end AS INTEGER)
       |)
       |
       """.stripMargin)

  before{
    val ref = spark.read.parquet(getClass.getResource("/refFlat.adam").getPath)
    ref.createOrReplaceTempView("ref")

    val snp = spark.read.parquet(getClass.getResource("/snp150Flagged.adam").getPath)
    snp.createOrReplaceTempView("snp")

  }
  test("Test metrics insert") {
   val mc = new  MetricsCollector(spark,"testMetrics")
    mc.initMetricsTable
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    mc.runAndCollectMetrics(
      "q_overlap_snp_ref_tsv",
      "spark_granges_it_bc_all",
      Array("snp","ref"),
      query
    )



     spark.sql("SELECT * FROM testMetrics").show(false)

  }


}
