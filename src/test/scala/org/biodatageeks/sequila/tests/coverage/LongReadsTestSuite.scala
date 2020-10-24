package org.biodatageeks.sequila.tests.coverage

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class LongReadsTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  val bamPath: String =
    getClass.getResource("/nanopore_guppy_slice.bam").getPath
  val splitSize = 30000
  val tableNameBAM = "reads"

  before {

    System.setSecurityManager(null)
    spark.sql(s"DROP TABLE IF EXISTS $tableNameBAM")
    spark.sql(s"""
         |CREATE TABLE $tableNameBAM
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)

  }
  test("BAM - Nanopore with guppy basecaller") {

    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    session.sparkContext
      .setLogLevel("WARN")
    val bdg = session.sql(s"SELECT * FROM ${tableNameBAM}")
    assert(bdg.count() === 150)
  }

  test("BAM - coverage - Nanopore with guppy basecaller") {
    spark.sqlContext.setConf(InternalParams.InputSplitSize,
                             (splitSize * 10).toString)
    val session2: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session2)
    val query =
      s"""SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.COVERAGE}
        FROM coverage('$tableNameBAM','nanopore_guppy_slice','bases')
        order by ${Columns.CONTIG},${Columns.START},${Columns.END}
        """.stripMargin
    val covMultiPartitionDF = session2.sql(query)

    //covMultiPartitionDF.coalesce(1).write.mode("overwrite").option("delimiter", "\t").csv("/Users/aga/workplace/multiPart.csv")
    assert(covMultiPartitionDF.count() == 45620) // total count check 45620<---> 45842

    assert(covMultiPartitionDF.filter(s"${Columns.COVERAGE}== 0").count == 0)

    assert(
      covMultiPartitionDF
        .where(s"${Columns.CONTIG}='21' and ${Columns.START} == 5010515")
        .first()
        .getShort(2) == 1) // value check [first element]
    assert(
      covMultiPartitionDF
        .where(s"${Columns.CONTIG}='21' and ${Columns.START} == 5022667")
        .first()
        .getShort(2) == 15) // value check [partition boundary]
    assert(
      covMultiPartitionDF
        .where(s"${Columns.CONTIG}='21' and ${Columns.START} == 5036398")
        .first()
        .getShort(2) == 14) // value check [partition boundary]
    assert(
      covMultiPartitionDF
        .where(s"${Columns.CONTIG}='21' and ${Columns.START} == 5056356")
        .first()
        .getShort(2) == 1) // value check [last element]

  }

}
