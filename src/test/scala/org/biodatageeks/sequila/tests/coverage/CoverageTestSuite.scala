package org.biodatageeks.sequila.tests.coverage

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.sequila.coverage.CoverageStrategy
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class CoverageTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  val bamPath: String = getClass.getResource("/NA12878.slice.bam").getPath
  val bamMultiPath: String =
    getClass.getResource("/multichrom/bam/NA12878.multichrom.bam").getPath
  val adamPath: String = getClass.getResource("/NA12878.slice.adam").getPath
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val cramPath: String = getClass.getResource("/cram/test.cram").getPath
  val refPath: String = getClass.getResource("/cram/test.fa").getPath
  val tableNameBAM = "reads"
  val tableNameMultiBAM = "readsMulti"
  val tableNameADAM = "readsADAM"
  val tableNameCRAM = "readsCRAM"
  val splitSize = "1000000"

  val alignReadMethods = Array("disq", "hadoopBAM")

  before {

    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    System.setSecurityManager(null)
    spark.sql(s"DROP TABLE IF EXISTS $tableNameBAM")
    spark.sql(s"""
         |CREATE TABLE $tableNameBAM
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS $tableNameMultiBAM")
    spark.sql(s"""
         |CREATE TABLE $tableNameMultiBAM
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamMultiPath")
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
         |
         |USING org.biodatageeks.sequila.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "$adamPath")
         |
      """.stripMargin)

  }

  /*

      for InputSplitSize = 1000000 and table tableNameMultiBAM partition boundaries are as follows:
      chr1 : 34
      chrM : 7
      chrM : 7882
      chrM : 14402

   */

  test("BAM - coverage - windows") {

    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)

    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    alignReadMethods.foreach { m =>
      spark.sqlContext.setConf(InternalParams.IOReadAlignmentMethod, m)
      val windowLength = 100
      val bdg = session.sql(
        s"SELECT * FROM coverage('$tableNameMultiBAM','NA12878', 'blocks', '$windowLength')")
      assert(bdg.count == 267)
      assert(bdg.first().getInt(1) % windowLength == 0) // check for fixed window start position
      assert(bdg.first().getInt(2) % windowLength == windowLength - 1) // // check for fixed window end position
      assert(
        bdg
          .where(s"${Columns.CONTIG} == '1' and ${Columns.START} == 2700")
          .first()
          .getFloat(3) == 4.65.toFloat)
      assert(
        bdg
          .where(s"${Columns.CONTIG} == '1' and ${Columns.START}  == 3200")
          .first()
          .getFloat(3) == 166.79.toFloat)
      assert(
        bdg
          .where(s"${Columns.CONTIG} == '1' and ${Columns.START} == 10000")
          .first()
          .getFloat(3) == 1.5522388.toFloat) //value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG} == 'MT' and ${Columns.START} == 7800")
          .first()
          .getFloat(3) == 253.03001.toFloat) //value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG} == 'MT' and ${Columns.START} == 14400")
          .first()
          .getFloat(3) == 134.7.toFloat) //value check [partition boundary]
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}", s"${Columns.START}")
          .count()
          .where("count != 1")
          .count == 0) // no duplicates check
    }
  }

  test("BAM - coverage - blocks - allPositions") {
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)

    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    alignReadMethods.foreach { m =>
      spark.sqlContext.setConf(InternalParams.IOReadAlignmentMethod, m)
      session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil

      session.sqlContext.setConf(InternalParams.ShowAllPositions, "true")

      val bdg = session.sql(
        s"SELECT * FROM coverage('${tableNameMultiBAM}','NA12878', 'blocks')")

      assert(bdg.count() == 12865)
      assert(bdg.filter("coverage== 0").count == 29) // count check fo zero coverage elements

      assert(bdg.first().get(1) == 1) // first position check (should start from 1 with ShowAllPositions = true)
      assert(
        bdg
          .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
          .first()
          .getShort(3) == 2) // value check
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7")
          .first()
          .getShort(3) == 1) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7881")
          .first()
          .getShort(3) == 248) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7882")
          .first()
          .getShort(3) == 247) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7883")
          .first()
          .getShort(3) == 246) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 14402")
          .first()
          .getShort(3) == 182) // value check [partition boundary]
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}")
          .max(Columns.END)
          .where(s"${Columns.CONTIG} == '1'")
          .first()
          .get(1) == 247249719) // max value check
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}")
          .max(Columns.END)
          .where(s"${Columns.CONTIG} == 'MT'")
          .first()
          .get(1) == 16571) // max value check
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}", s"${Columns.START}")
          .count()
          .where("count != 1")
          .count == 0) // no duplicates check
    }
  }

  test("BAM - coverage - blocks notAllPositions") {
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    session.sqlContext.setConf(InternalParams.ShowAllPositions, "false")

    alignReadMethods.foreach { m =>
      spark.sqlContext.setConf(InternalParams.IOReadAlignmentMethod, m)

      val bdg = session.sql(
        s"SELECT *  FROM coverage('${tableNameMultiBAM}','NA12878', 'blocks')")

      assert(bdg.count() == 12836) // total count check // old value before zero_elements fix
      assert(bdg.filter("coverage== 0").count == 0)

      assert(bdg.first().get(1) != 1) // first position check (should not start from 1 with ShowAllPositions = false)
      assert(
        bdg
          .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
          .first()
          .getShort(3) == 2) // value check
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7")
          .first()
          .getShort(3) == 1) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7881")
          .first()
          .getShort(3) == 248) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7882")
          .first()
          .getShort(3) == 247) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7883")
          .first()
          .getShort(3) == 246) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 14402")
          .first()
          .getShort(3) == 182) // value check [partition boundary]
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}")
          .max(Columns.END)
          .where(s"${Columns.CONTIG} == '1'")
          .first()
          .get(1) == 10066) // max value check
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}")
          .max(Columns.END)
          .where(s"${Columns.CONTIG} == 'MT'")
          .first()
          .get(1) == 16571) // max value check
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}", s"${Columns.START}")
          .count()
          .where("count != 1")
          .count == 0) // no duplicates check
    }
  }

  test("BAM - coverage - bases - notAllPositions") {
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    session.sqlContext.setConf(InternalParams.ShowAllPositions, "false")
    alignReadMethods.foreach { m =>
      spark.sqlContext.setConf(InternalParams.IOReadAlignmentMethod, m)
      val bdg = session.sql(
        s"SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.COVERAGE} FROM coverage('${tableNameMultiBAM}','NA12878', 'bases')")

      assert(bdg.count() == 24898) // total count check // was 26598
      assert(bdg.filter(s"${Columns.COVERAGE}== 0").count == 0) // total count of zero coverage elements should be zero

      assert(bdg.first().get(1) != 1) // first position check (should not start from 1 with ShowAllPositions = false)
      assert(
        bdg
          .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
          .first()
          .getShort(3) == 2) // value check
      assert(
        bdg.where(s"${Columns.CONTIG}='1' and ${Columns.START} == 88").first().getShort(3) == 7)
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7")
          .first()
          .getShort(3) == 1) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7881")
          .first()
          .getShort(3) == 248) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7882")
          .first()
          .getShort(3) == 247) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7883")
          .first()
          .getShort(3) == 246) // value check [partition boundary]
      assert(
        bdg
          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 14402")
          .first()
          .getShort(3) == 182) // value check [partition boundary]
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}")
          .max(s"${Columns.END}")
          .where(s"${Columns.CONTIG} == '1'")
          .first()
          .get(1) == 10066) // max value check
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}")
          .max(s"${Columns.END}")
          .where(s"${Columns.CONTIG} == 'MT'")
          .first()
          .get(1) == 16571) // max value check
      assert(
        bdg
          .groupBy(s"${Columns.CONTIG}", s"${Columns.START}")
          .count()
          .where("count != 1")
          .count == 0) // no duplicates check
    }

  }

  test("CRAM - coverage - show") {
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    alignReadMethods.foreach { m =>
      spark.sqlContext.setConf(InternalParams.IOReadAlignmentMethod, m)
      val bdg = session.sql(
        s"SELECT * FROM coverage('${tableNameCRAM}','test', 'blocks') ")

      assert(bdg.count() == 49)
      assert(bdg.where(s"${Columns.START} == 107").first().getShort(3) == 459)
    }
  }

  test("BAM - coverage - wrong param, Exception should be thrown") {
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    assertThrows[Exception](session
      .sql(
        s"SELECT * FROM coverage('${tableNameMultiBAM}','NA12878', 'babara')")
      .show())

  }

  after {

    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    Metrics.stopRecording()

  }

}
