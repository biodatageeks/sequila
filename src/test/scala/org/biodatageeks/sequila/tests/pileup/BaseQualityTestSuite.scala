package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.{DataFrame, SequilaSession}
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}

class BaseQualityTestSuite extends PileupTestBase {

  val splitSize = "1000000"
  val qualCoverageCol = "qual_coverage"
  val covEquality = "cov_equal"
  val qualAgg = "qual_map"
  val pileupQuery =
    s"""
       |SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END},
       | ${Columns.REF}, ${Columns.COVERAGE},
       | alts_to_char(${Columns.ALTS}) as alts, ${Columns.QUALS},
       | quals_to_cov(${Columns.QUALS}, ${Columns.COVERAGE}) as $qualCoverageCol,
       | to_charmap(${Columns.QUALS}) as $qualAgg,
       | cov_equals (${Columns.COVERAGE}, ${Columns.COVERAGE} ) as $covEquality
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true, true)
       |ORDER BY ${Columns.CONTIG}
                 """.stripMargin
  
  test("Simple Quals lookup Single partition") {
    val conf = new Conf
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")

    val result = ss.sql(pileupQuery)
    result.show()
    val equals = result.select(covEquality).distinct()
    result.where(s"$covEquality == false").show(10, false)

    assert(equals.count()==1)
    assert(equals.head.getBoolean(0))
    assert(!conf.isBinningEnabled)
  }

  test("Simple Quals lookup Multiple partitions") {
    val conf = new Conf
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")

    val result = ss.sql(pileupQuery)

    val partNum = result.rdd.getNumPartitions
    val count = result.count()
    assert(count >= 14671 && count <= 14671 + partNum)

    result.where(s"$covEquality == false").show(10, false)

    val equals = result.select(covEquality).distinct()
    result.where(s"$covEquality=false").show(false)

    assert(equals.count()==1)
    assert(equals.head.getBoolean(0))
    assert(!conf.isBinningEnabled)
  }
}
