package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.{DataFrame, SequilaSession}
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}

class PileupTestSuite extends PileupTestBase {

  val splitSize = "1000000"

  val pileupQuery =
    s"""
       |SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.REF}, ${Columns.COVERAGE}, ${Columns.ALTS}
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath')
       | order by ${Columns.CONTIG}
                 """.stripMargin

  
  test("Normal split") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")

    val result = ss.sql(pileupQuery)
//    Writer.saveToFile(ss, result, "bdgNorm.csv")
//    result.where(s"${Columns.CONTIG}='MT' and ${Columns.START} >= 7").show(20)
    performAssertions(result)
  }

  test("Pileup  changed split size") {
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val result = ss.sql(pileupQuery)
//    Writer.saveToFile(ss, result, "bdgSplit.csv")
//    result.where(s"${Columns.CONTIG}='MT' and ${Columns.START} >= 7 and ${Columns.START} <= 50").show(20)
    performAssertions(result)


  }

  private def performAssertions(result:DataFrame):Unit ={
    assert(result.count()==14671)
    assert(result.first().get(1) != 1) // first position check (should not start from 1 with ShowAllPositions = false)
    assert(
      result
        .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
        .first()
        .getShort(4) == 2) // value check
    assert(
      result.where(s"${Columns.CONTIG}='1' and ${Columns.START} == 88").first().getShort(4) == 7)

    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7")
        .first()
        .getShort(4) == 1) // value check [partition boundary]
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7881")
        .first()
        .getShort(4) == 248) // value check [partition boundary]
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7882")
        .first()
        .getShort(4) == 247) // value check
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7883")
        .first()
        .getShort(4) == 246) // value check
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 14402")
        .first()
        .getShort(4) == 182) // value check

    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7830")
        .first()
        .getShort(4) == 252) // value check [partition boundary]

    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7831")
        .first()
        .getShort(4) == 259) // value check [partition boundary]
    assert(
      result
        .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7832")
        .first()
        .getShort(4) == 264) // value check [partition boundary]
    assert(
      result
        .groupBy(s"${Columns.CONTIG}")
        .max(s"${Columns.END}")
        .where(s"${Columns.CONTIG} == '1'")
        .first()
        .get(1) == 10066) // max value check
    assert(
      result
        .groupBy(s"${Columns.CONTIG}")
        .max(s"${Columns.END}")
        .where(s"${Columns.CONTIG} == 'MT'")
        .first()
        .get(1) == 16571) // max value check
    assert(
      result
        .groupBy(s"${Columns.CONTIG}", s"${Columns.START}")
        .count()
        .where("count != 1")
        .count == 0) // no duplicates check
    assert(
      result
        .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 34")
        .first()
        .getString(3) == "C") // Ref check
    assert(
      result
        .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
        .first()
        .getString(3) == "C") // Ref check
    assert(
      result
        .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 36")
        .first()
        .getString(3) == "CT") // Ref check
  }
}
