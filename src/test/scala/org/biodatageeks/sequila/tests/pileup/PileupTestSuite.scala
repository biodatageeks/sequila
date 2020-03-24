package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.base.BAMBaseTestSuite
import org.biodatageeks.sequila.utils.{Columns, SequilaRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class PileupTestSuite  extends FunSuite
  with DataFrameSuiteBase
  with BeforeAndAfter
  with SharedSparkContext {

  val bamMultiPath: String =
    getClass.getResource("/multichrom/bam/NA12878.multichrom.bam").getPath

  val tableNameMultiBAM = "readsMulti"

  val bamPath: String = getClass.getResource("/NA12878.slice.md.bam").getPath
  val tableNameBAM = "reads"


  before {

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
  }

  test("Pileup mock"){
    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")
    val query =
      s"""
         |SELECT *
         |FROM  pileup('$tableNameMultiBAM')
       """.stripMargin
    val result = ss.sql(query)
    result.show(10,false)

//    val tmp = result.where(s"${Columns.CONTIG}='MT'").takeAsList(100)
//    assert(result.count() == 17267)

    assert(result.first().get(1) != 1) // first position check (should not start from 1 with ShowAllPositions = false)
    assert(
      result
        .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
        .first()
        .getShort(3) == 2) // value check
    assert(
      result.where(s"${Columns.CONTIG}='1' and ${Columns.START} == 88").first().getShort(3) == 7)

    result.where(s"${Columns.CONTIG}='MT' and ${Columns.START} < 15").show()
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7")
        .first()
        .getShort(3) == 1) // value check [partition boundary]
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7881")
        .first()
        .getShort(3) == 248) // value check [partition boundary]
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7882")
        .first()
        .getShort(3) == 247) // value check [partition boundary]
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7883")
        .first()
        .getShort(3) == 246) // value check [partition boundary]
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 14402")
        .first()
        .getShort(3) == 182) // value check [partition boundary]
    assert(
      result
        .groupBy(s"${Columns.CONTIG}")
        .max(s"${Columns.START}")
        .where(s"${Columns.CONTIG} == '1'")
        .first()
        .get(1) == 10066) // max value check
    assert(
      result
        .groupBy(s"${Columns.CONTIG}")
        .max(s"${Columns.START}")
        .where(s"${Columns.CONTIG} == 'MT'")
        .first()
        .get(1) == 16571) // max value check
    assert(
      result
        .groupBy(s"${Columns.CONTIG}", s"${Columns.START}")
        .count()
        .where("count != 1")
        .count == 0) // no duplicates check
  }




//  test("Pileup multi chrom check"){
//    val  ss = SequilaSession(spark)
//    SequilaRegister.register(ss)
//    ss.sparkContext.setLogLevel("WARN")
//    val query =
//      s"""
//         |SELECT distinct contig
//         |FROM  pileup('$tableNameMultiBAM')
//       """.stripMargin
//    val result = ss.sql(query)
//    assert(result.count() == 2)
//
//
//  }
}
