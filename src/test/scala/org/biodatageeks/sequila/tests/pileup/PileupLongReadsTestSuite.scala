package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.{Columns, SequilaRegister}

class PileupLongReadsTestSuite extends PileupTestBase{
  test("Pileup without BQ") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val query =
      s"""
         |SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.REF}, ${Columns.COVERAGE},${Columns.ALTS}
         |FROM  pileup('${tableNameLongReads}', '${sampleLongReadsId}' , '$referenceLongReadsPath', false)
                 """.stripMargin

    ss.sql(query).count

  }

  test("Pileup with BQ") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val query =
      s"""
         |SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.REF}, ${Columns.COVERAGE},${Columns.ALTS}
         |FROM  pileup('${tableNameLongReads}', '${sampleLongReadsId}' , '$referenceLongReadsPath', true)
                 """.stripMargin

    ss.sql(query).count

  }

}
