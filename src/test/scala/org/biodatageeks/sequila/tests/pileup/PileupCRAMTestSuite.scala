package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.{Columns}

class PileupCRAMTestSuite extends PileupTestBase {

  test("Compare BAM and CRAM results") {
    val ss = SequilaSession(spark)
    val query =
      s"""
         |SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.REF}, ${Columns.COVERAGE},${Columns.ALTS}
         |FROM  pileup('{{tableName}}', '${sampleId}' , '$referencePath', true)
                 """.stripMargin
    val resultBAM = ss
      .sql(query.replace("{{tableName}}", tableName))
    val resultCRAM = ss
      .sql(query.replace("{{tableName}}", tableNameCRAM))

    assert(resultBAM.count() === resultCRAM.count())
    assertDataFrameEquals(resultBAM, resultCRAM)
  }
}
