package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}

class BinningTestSuite extends PileupTestBase {

  val splitSize = "1000000"

  val qualCoverageCol = "qual_coverage"
  val covEquality = "cov_equal"
  val qualAgg = "qual_map"

  val binSize = 2

  val pileupQuery =
    s"""
       |SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END},
       | ${Columns.REF}, ${Columns.COVERAGE},
       | ${Columns.ALTS}, ${Columns.QUALS},
       | quals_to_cov(${Columns.QUALS}, ${Columns.COVERAGE}) as $qualCoverageCol,
       | quals_to_map(${Columns.QUALS}) as $qualAgg,
       | cov_equals (${Columns.COVERAGE}, ${Columns.COVERAGE}) as $covEquality
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true, true, $binSize)
       |ORDER BY ${Columns.CONTIG}
                 """.stripMargin

  test("Simple Quals lookup Single partition") {
    val conf = new Conf
    conf.isBinningEnabled = true
    conf.binSize = binSize
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")

    val result = ss.sql(pileupQuery)

    val equals = result.select(covEquality).distinct()
    assert(equals.count()==1)
    assert(equals.head.getBoolean(0))
    assert(conf.isBinningEnabled)
    assert(conf.binSize == binSize )
  }

  test("Simple Quals lookup Multiple partitions") {
    val conf = new Conf
    conf.isBinningEnabled = true
    conf.binSize = binSize
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")

    val result = ss.sql(pileupQuery)

    val partNum = result.rdd.getNumPartitions
    val count = result.count()
    assert(count >= 14671 && count <= 14671 + partNum)

    val equals = result.select(covEquality).distinct()
    assert(equals.count()==1)
    assert(equals.head.getBoolean(0))
    assert(conf.isBinningEnabled)
    assert(conf.binSize == binSize )
  }
}
