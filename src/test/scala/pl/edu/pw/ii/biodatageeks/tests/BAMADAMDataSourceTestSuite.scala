package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.scalatest.{BeforeAndAfter, FunSuite}

class BAMADAMDataSourceTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{


  val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
  val adamPath = getClass.getResource("/NA12878.slice.adam").getPath
  val cramPath = getClass.getResource("/test.cram").getPath
  val refPath = getClass.getResource("/phix-illumina.fa").getPath
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val tableNameBAM = "reads"
  val tableNameADAM = "readsADAM"
  val tableNameCRAM = "readsCRAM"
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

    spark.sql(s"DROP TABLE IF EXISTS ${tableNameCRAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameCRAM}
         |USING org.biodatageeks.datasources.BAM.CRAMDataSource
         |OPTIONS(path "${cramPath}", refPath "${refPath}")
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

  test("BAM - select limit" ){

    spark
      .sql(s"SELECT contigName,start,end FROM ${tableNameBAM} limit 1").show()
  }

  test("BAM - Row count ADAMDataSource2"){
    spark
      .sql(s"SELECT contigName,start,end FROM ${tableNameADAM}").show(1)
      //.count === 3172L)
  }

  test("CRAM - select limit" ){

    spark
      .sql(s"SELECT contigName,start,end,cigar FROM ${tableNameCRAM}").show(10)
  }

  test("CRAM - select count" ){

    assert(spark
      .sql(s"SELECT * FROM ${tableNameCRAM}").count() == 2860L )
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

  test("BAM - select only sampleId"){
    assert(spark
      .sql(s"SELECT distinct sampleId FROM ${tableNameBAM} order by sampleId")
      .first().getString(0) == "NA12878")
  }

  after{
    spark.sql(s"DROP TABLE IF EXISTS  ${tableNameBAM}")
    writer.flush()
  }

}
