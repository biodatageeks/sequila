package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.preprocessing.coverage.CoverageStrategy
import org.biodatageeks.utils.{BDGInternalParams, SequilaRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class CoverageTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{

    val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
    val bamMultiPath = getClass.getResource("/multichrom/NA12878.multichrom.bam").getPath
    val adamPath = getClass.getResource("/NA12878.slice.adam").getPath
    val metricsListener = new MetricsListener(new RecordedMetrics())
    val writer = new PrintWriter(new OutputStreamWriter(System.out))
    val cramPath = getClass.getResource("/test.cram").getPath
    val refPath = getClass.getResource("/phix-illumina.fa").getPath
    val tableNameBAM = "reads"
    val tableNameMultiBAM = "readsMulti"
    val tableNameADAM = "readsADAM"
    val tableNameCRAM = "readsCRAM"
    before{

      Metrics.initialize(sc)
      sc.addSparkListener(metricsListener)
      System.setSecurityManager(null)
      spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
      spark.sql(
        s"""
           |CREATE TABLE ${tableNameBAM}
           |USING org.biodatageeks.datasources.BAM.BAMDataSource
           |OPTIONS(path "${bamPath}")
           |
      """.stripMargin)

      spark.sql(s"DROP TABLE IF EXISTS ${tableNameMultiBAM}")
      spark.sql(
        s"""
           |CREATE TABLE ${tableNameMultiBAM}
           |USING org.biodatageeks.datasources.BAM.BAMDataSource
           |OPTIONS(path "${bamMultiPath}")
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
  test("BAM - coverage table-valued function"){
    val session: SparkSession = SequilaSession(spark)

    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil
    session.sql(s"SELECT * FROM coverage('${tableNameBAM}') WHERE position=20204").count()
    assert(session.sql(s"SELECT * FROM coverage('${tableNameBAM}') WHERE position=20204").first().getInt(3)===1019)
    //session.sql(s"SELECT * FROM coverage_hist('${tableNameBAM}') WHERE position=20204").show()

  }

  test("BAM - bdg_coverage - block - allPositions"){
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    //session.sparkContext.setLogLevel("DEBUG")
    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil


    session.sqlContext.setConf(BDGInternalParams.ShowAllPositions,"true")
    val bdg = session.sql(s"SELECT * FROM bdg_coverage('${tableNameMultiBAM}','NA12878','bdg', 'blocks')")
    bdg.show()
    assert(bdg.first().get(1)==1) // first position should be one

  }

  test("BAM - bdg_coverage - block "){
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    //session.sparkContext.setLogLevel("DEBUG")

    session.sqlContext.setConf(BDGInternalParams.ShowAllPositions,"false")
    val bdg = session.sql(s"SELECT *  FROM bdg_coverage('${tableNameMultiBAM}','NA12878','bdg', 'blocks')")
    bdg.show()

    assert(bdg.first().get(1)!=1) // first position should not be one

  }

  test("BAM - bdg_coverage - base - show"){
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    session.sqlContext.setConf(BDGInternalParams.ShowAllPositions,"false")
    val bdg = session.sql(s"SELECT contigName, start, coverage FROM bdg_coverage('${tableNameMultiBAM}','NA12878','bdg', 'bases')")
    bdg.show()
    assert(bdg.first().get(1)!=1) // first position should not be one
  }

  test("BAM - bdg_coverage - block - no configuration"){
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    val bdg = session.sql(s"SELECT * FROM bdg_coverage('${tableNameMultiBAM}','NA12878','bdg', 'blocks')")
    assert(bdg.first().get(1)!=1) // first position should not be one

  }

  test("BAM - bdg_coverage - base - count"){
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    session.sql(s"SELECT contigName, start, coverage FROM bdg_coverage('${tableNameMultiBAM}','NA12878','bdg', 'bases')").count

  }

  test("BAM - bdg_coverage - blocks - count"){
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    session.sql(s"SELECT contigName, start, coverage FROM bdg_coverage('${tableNameMultiBAM}','NA12878','bdg', 'blocks')").count
  }

  test("BAM - bdg_coverage - wrong param") {
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    assertThrows[Exception](
      session.sql(s"SELECT * FROM bdg_coverage('${tableNameMultiBAM}','NA12878','bdg', 'blaaaaaah')").show(10))

  }

  test("CRAM - bdg_coverage - show"){
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    session.sql(s"SELECT * FROM bdg_coverage('${tableNameCRAM}','test','bdg', 'blocks') ").show(5)

  }
  after{

    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    Metrics.stopRecording()
    //writer.

  }

}
