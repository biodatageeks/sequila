package org.biodatageeks.sequila.tests.optimizations

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.base.BAMBaseTestSuite
import org.biodatageeks.sequila.utils.{Columns}

class SequilaDatasourceStrategyTestSuite extends BAMBaseTestSuite with SharedSparkContext {


    test("Test query with distinct sample optimization") {
      val ss = SequilaSession(spark)
      assert(
        ss.sql(s"SELECT distinct ${Columns.SAMPLE} FROM $tableNameBAM LIMIT 10")
          .count() === 1)

      assert(
        ss.sql(s"SELECT distinct ${Columns.SAMPLE} FROM $tableNameBAM LIMIT 10")
          .first()
          .getString(0) === "NA12878")
    }

  test("TEST query all columns with LIMIT optimization") {
    val ss = SequilaSession(spark)
    ss.sparkContext.setLogLevel("INFO")
    val sqlText = s"SELECT * FROM $tableNameBAM LIMIT 10"
    ss.time {
      ss
        .sql(sqlText)
        .show
    }

  }

  test("TEST query subset columns with LIMIT optimization") {
    val ss = SequilaSession(spark)
    ss.sparkContext.setLogLevel("INFO")
    val sqlText = s"SELECT ${Columns.QNAME},${Columns.SEQUENCE},${Columns.BASEQ} FROM $tableNameBAM LIMIT 10"
    ss.time {
      ss
        .sql(sqlText)
        .show
    }
  }
}
