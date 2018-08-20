package pl.edu.pw.ii.biodatageeks.tests

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.utils.SequilaRegister

class GenomicIntervalTVFTestSuite  extends  BAMBaseTestSuite {


test("Test bdg_grange TVF"){
  val  ss = SequilaSession(spark)
  SequilaRegister.register(ss)
  val query =
    s"""
       |SELECT count(*) FROM  ${tableNameBAM} r JOIN (SELECT * FROM bdg_grange('chr1',34,110)) as gi
       |ON (r.contigName=gi.contigName
       |AND
       |gi.end>=r.start
       |AND
       |gi.start<=r.end
       |)
       |
       """.stripMargin

  assert(ss
    .sql(query)
        .first()
    .getLong(0) === 7)

}



}
