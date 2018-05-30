package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.scalatest.{BeforeAndAfter, FunSuite}

class BAMADAMDataSourceTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{


  val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
  val adamPath = getClass.getResource("/NA12878.slice.adam").getPath
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val tableNameBAM = "reads"
  val tableNameADAM = "readsADAM"
  before{

    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS ${tableNameADAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameADAM}
         |USING org.biodatageeks.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "${adamPath}")
         |
      """.stripMargin)


  }
  test("BAM - Row count BAMDataSource"){
    assert(spark
      .sql(s"SELECT * FROM ${tableNameBAM}")
      .count === 3172L)
  }


  test("ADAM - Row count BAMDataSource"){
    assert(spark
      .sql(s"SELECT * FROM ${tableNameADAM}")
      .count === 3172L)
  }

  test("IntervalTree strategy over BAMDataSource"){
    val targets = spark
      .sqlContext
      .createDataFrame(Array(Region("chr1",20138,20294)))
    targets
      .createOrReplaceTempView("targets")
    val query =s"""SELECT count(*),targets.contigName,targets.start,targets.end
              FROM ${tableNameBAM} reads JOIN targets
        |ON (
        |  targets.contigName=reads.contigName
        |  AND
        |  reads.end >= targets.start
        |  AND
        |  reads.start <= targets.end
        |)
        |GROUP BY sampleId,targets.contigName,targets.start,targets.end
        |having contigName='chr1' AND    start=20138 AND  end=20294""".stripMargin

    spark
      .sql(query)
      .explain(false)
    spark.sql(query).show()
   // assert(spark.sql(query).first().getLong(0) === 1484L)

  }

  test("BAM - coverage BAMDataSource"){
    assert(spark
      .sql(s"SELECT * FROM ${tableNameBAM}")
      .count === 3172L)
  }

  after{
    spark.sql(s"DROP TABLE IF EXISTS  ${tableNameBAM}")
    writer.flush()
  }

}
