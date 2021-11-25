package org.biodatageeks.sequila.tests.rangejoins

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.base.BAMBaseTestSuite
import org.biodatageeks.sequila.utils.{Columns}

class GenomicIntervalTVFTestSuite  extends  BAMBaseTestSuite {


test("Test bdg_grange TVF"){

  val  ss = SequilaSession(spark)
  ss.sparkContext.setLogLevel("WARN")

  val query =
    s"""
       |SELECT count(*)
       |FROM  $tableNameBAM AS r JOIN (SELECT * FROM bdg_grange('1',34,110)) as gi
       |ON (r.${Columns.CONTIG} = gi.${Columns.CONTIG}
       |AND gi.${Columns.END} >= r.${Columns.START}
       |AND gi.${Columns.START} <= r.${Columns.END}
       |)
       |
       """.stripMargin

  ss.sql(query).explain(true)
  assert(ss
    .sql(query)
        .first()
    .getLong(0) === 7)

}



}
