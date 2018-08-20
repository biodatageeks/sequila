package pl.edu.pw.ii.biodatageeks.tests

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.utils.SequilaRegister

class SequilaDatasourceStrategyTestSuite  extends  BAMBaseTestSuite {

  test("Simple select over a BAM table") {
    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    assert(
      ss
      .sql(s"SELECT distinct sampleId FROM ${tableNameBAM} LIMIT 10")
        .count() === 1)

    assert(
      ss
        .sql(s"SELECT distinct sampleId FROM ${tableNameBAM} LIMIT 10")
        .first()
      .getString(0) === "NA12878")
  }

  test("Simple select over a BAM table group by") {
    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    assert(ss
      .sql(s"SELECT sampleId,count(*) FROM ${tableNameBAM} group by sampleId")
      .first()
      .getLong(1) === 3172)
  }

}
