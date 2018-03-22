package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.scalatest.{BeforeAndAfter, FunSuite}

class MultisampleBAMTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext {


  val bamPath = getClass.getResource("/multisample").getPath+"/NA1287*.bam"
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val tableNameBAM = "reads"

  before{
    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)

  }

  test("Multisample BAM Table"){
    assert(spark
      .sql(s"SELECT * FROM ${tableNameBAM}")
      .count === 6344L)
  }

  test("Sample name"){
    assert(spark
      .sql(s"SELECT sampleId FROM ${tableNameBAM}")
      .first.getString(0) === "NA12878")
  }

  test("Feature counts with multiple samples"){
    val query ="""SELECT sampleId,count(*),targets.contigName,targets.start,targets.end
              FROM reads JOIN targets
        |ON (
        |  targets.contigName=reads.contigName
        |  AND
        |  reads.end >= targets.start
        |  AND
        |  reads.start <= targets.end
        |)
        |GROUP BY sampleId,targets.contigName,targets.start,targets.end
        |having contigName='chr1' AND  start=20138 AND  end=20294 and sampleId='NA12878'""".stripMargin
    val targets = spark
      .sqlContext
      .createDataFrame(Array(Region("chr1",20138,20294)))
    targets
      .createOrReplaceTempView("targets")
    assert(spark.sql(query).first().getLong(1) === 1484L)
  }

}
