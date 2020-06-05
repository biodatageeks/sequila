package org.biodatageeks.sequila.tests.datasources

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.log4j.Logger
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.sequila.apps.FeatureCounts.Region
import org.biodatageeks.sequila.utils.Columns
import org.scalatest.{BeforeAndAfter, FunSuite}


class BAMADAMDataSourceTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  val bamPath: String = getClass.getResource("/NA12878.slice.bam").getPath
  val adamPath: String = getClass.getResource("/NA12878.slice.adam").getPath
  val cramPath: String = getClass.getResource("/NA12878.slice.cram").getPath
  val refPath: String = getClass.getResource("/NA12878.slice.fasta").getPath
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val tableNameBAM = "reads"
  val tableNameADAM = "readsADAM"
  val tableNameCRAM = "readsCRAM"
  before {

    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    spark.sql(s"DROP TABLE IF EXISTS $tableNameBAM")
    spark.sql(s"""
         |CREATE TABLE $tableNameBAM
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS $tableNameCRAM")
    spark.sql(s"""
         |CREATE TABLE $tableNameCRAM
         |USING org.biodatageeks.sequila.datasources.BAM.CRAMDataSource
         |OPTIONS(path "$cramPath", refPath "$refPath")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS $tableNameADAM")
    spark.sql(s"""
         |CREATE TABLE $tableNameADAM
         |USING org.biodatageeks.sequila.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "$adamPath")
         |
      """.stripMargin)

  }
  test("BAM - Row count BAMDataSource") {
    assert(
      spark
        .sql(s"SELECT * FROM $tableNameBAM")
        .count === 3172L)
  }

  test("BAM - select limit") {

    val sqlText = s"SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END} FROM $tableNameBAM limit 1"
    val log = Logger.getLogger("TEST")
    log.warn(sqlText)
    spark
      .sql(sqlText)
      .printSchema()
  }


  test("BAM - Row count ADAMDataSource2") {
    assert(
      spark
        .sql(s"SELECT * FROM $tableNameADAM")
        .count === 3172L)
  }

  test("CRAM - select limit") {

    spark
      .sql(
        s"SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.CIGAR} FROM $tableNameCRAM")
      .show(10)
  }

  test("CRAM - select count") {

    assert(
      spark
        .sql(s"SELECT * FROM $tableNameCRAM")
        .count() == 3172L)
  }

  test("ADAM - Row count BAMDataSource") {
    assert(
      spark
        .sql(s"SELECT * FROM $tableNameADAM")
        .count === 3172L)
  }

  test("IntervalTree strategy over BAMDataSource") {
    val targets = spark.sqlContext
      .createDataFrame(Array(Region("1", 20138, 20294)))
      .withColumnRenamed("contigName", Columns.CONTIG)
      .withColumnRenamed("start", Columns.START)
      .withColumnRenamed("end", Columns.END)
    targets
      .createOrReplaceTempView("targets")
    val query =
      s"""SELECT count(*),targets.${Columns.CONTIG},targets.${Columns.START},targets.${Columns.END}
              FROM $tableNameBAM reads JOIN targets
        |ON (
        |  targets.${Columns.CONTIG} = reads.${Columns.CONTIG}
        |  AND
        |  reads.${Columns.END} >= targets.${Columns.START}
        |  AND
        |  reads.${Columns.START} <= targets.${Columns.END}
        |)
        |GROUP BY ${Columns.SAMPLE}, targets.${Columns.CONTIG}, targets.${Columns.START}, targets.${Columns.END}
        |HAVING ${Columns.CONTIG} = '1' AND  ${Columns.START} = 20138 AND  ${Columns.END} = 20294""".stripMargin

    spark
      .sql(query)
      .explain(false)
    spark.sql(query).show()
    assert(spark.sql(query).first().getLong(0) === 1484L)

  }

  test("BAM - coverage BAMDataSource") {
    assert(
      spark
        .sql(s"SELECT * FROM $tableNameBAM")
        .count === 3172L)
  }

  test("BAM - select only sampleId") {
    assert(
      spark
        .sql(
          s"SELECT distinct ${Columns.SAMPLE} FROM $tableNameBAM order by ${Columns.SAMPLE}")
        .first()
        .getString(0) == "NA12878")
  }

  test("BAM - select only bases") {
    spark
      .sql(s"SELECT ${Columns.SEQUENCE} FROM $tableNameBAM limit 5")
      .show()
  }

  after {
    spark.sql(s"DROP TABLE IF EXISTS  $tableNameBAM")
    writer.flush()
    Metrics.stopRecording()
  }

}
