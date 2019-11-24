package org.biodatageeks.sequila.tests.datasources

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.{Columns, SequilaRegister}

class SequilaDatasourceStrategyTestSuite extends BAMBaseTestSuite {

  test("Simple select over a BAM table") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    assert(
      ss.sql(s"SELECT distinct ${Columns.SAMPLE} FROM $tableNameBAM LIMIT 10")
        .count() === 1)

    assert(
      ss.sql(s"SELECT distinct ${Columns.SAMPLE} FROM $tableNameBAM LIMIT 10")
        .first()
        .getString(0) === "NA12878")
  }

  test("Simple select over a BAM table group by") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    assert(
      ss.sql(
          s"SELECT ${Columns.SAMPLE},count(*) FROM $tableNameBAM group by ${Columns.SAMPLE}")
        .first()
        .getLong(1) === 3172)
  }

}
