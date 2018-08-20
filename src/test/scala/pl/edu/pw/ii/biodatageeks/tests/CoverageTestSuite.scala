package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.preprocessing.coverage.CoverageStrategy
import org.biodatageeks.utils.SequilaRegister
import org.scalatest.{BeforeAndAfter, FunSuite}

class CoverageTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{

    val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
    val adamPath = getClass.getResource("/NA12878.slice.adam").getPath
    val metricsListener = new MetricsListener(new RecordedMetrics())
    val writer = new PrintWriter(new OutputStreamWriter(System.out))
    val cramPath = getClass.getResource("/test.cram").getPath
    val refPath = getClass.getResource("/phix-illumina.fa").getPath
    val tableNameBAM = "reads"
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
    //session.sparkContext.setLogLevel("INFO")
    assert(session.sql(s"SELECT * FROM coverage('${tableNameBAM}') WHERE position=20204").first().getInt(3)===1019)
    session.sql(s"SELECT * FROM coverage_hist('${tableNameBAM}') WHERE position=20204").show()

  }

  test("BAM - bdg_coverage"){
    val session: SparkSession = SequilaSession(spark)
    session.sql(s"DESC FORMATTED ${tableNameBAM}").show(1000,false)
    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil
    assert(session.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878','mosdepth') WHERE start >=20204 AND `end`<= 20204 ").first().getShort(3)===1019.toShort)

  }

  test("BAM - bdg_coverage - show"){
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil
    session.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878','bdg')").show(5)

  }

  test("CRAM - bdg_coverage - show"){
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil
    session.sql(s"SELECT * FROM bdg_coverage('${tableNameCRAM}','test','bdg')").show(5)

  }
  after{

    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    Metrics.stopRecording()
    //writer.

  }

}
