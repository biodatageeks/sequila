package org.biodatageeks.sequila.tests.rangejoins

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.datasources.BAMBaseTestSuite
import org.biodatageeks.sequila.utils.{Columns, SequilaRegister}

class GenomicIntervalTVFTestSuite  extends  BAMBaseTestSuite {


test("Test bdg_grange TVF"){
  val  ss = SequilaSession(spark)
  SequilaRegister.register(ss)
  val query =
    s"""
       |SELECT count(*)
       |FROM  $tableNameBAM r JOIN (SELECT * FROM bdg_grange('chr1',34,110)) as gi
       |ON (r.${Columns.CONTIG} = gi.${Columns.CONTIG}
       |AND gi.${Columns.END} >= r.${Columns.START}
       |AND gi.${Columns.START} <= r.${Columns.END}
       |)
       |
       """.stripMargin

  assert(ss
    .sql(query)
        .first()
    .getLong(0) === 7)

}



}
