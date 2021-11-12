package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.{DataFrame, SequilaSession}
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.sequila.pileup.PileupPlan
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}

class ApiTestSuite extends PileupTestBase {

  val splitSize = "1000000"

  val covQuery =
    s"""
       |SELECT *
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath')
       | order by ${Columns.CONTIG}
                 """.stripMargin


  val altsQuery =
    s"""
       |SELECT *
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true)
       | order by ${Columns.CONTIG}
                 """.stripMargin

  val altsQualsQuery =
    s"""
       |SELECT *
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true, true)
       | order by ${Columns.CONTIG}
                 """.stripMargin


  test("Cov only") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val result = ss.sql(covQuery)

    assert(result.columns.contains(Columns.COVERAGE))
    assert(!result.columns.contains(Columns.ALTS))
    assert(!result.columns.contains(Columns.QUALS))
  }

  test("Alts") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val result = ss.sql(altsQuery)
    assert(result.columns.contains(Columns.COVERAGE))
    assert(result.columns.contains(Columns.ALTS))
    assert(!result.columns.contains(Columns.QUALS))
  }

  test("Alts quals") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val result = ss.sql(altsQualsQuery)
    assert(result.columns.contains(Columns.COVERAGE))
    assert(result.columns.contains(Columns.ALTS))
    assert(result.columns.contains(Columns.QUALS))
  }

}
