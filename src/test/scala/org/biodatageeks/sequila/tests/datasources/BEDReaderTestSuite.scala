package org.biodatageeks.sequila.tests.datasources

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.base.BEDBaseTestSuite

class BEDReaderTestSuite extends BEDBaseTestSuite with SharedSparkContext {

  test("Read BED file") {

    val ss = SequilaSession(spark)
    val sqlText = s"SELECT * FROM ${tableNameBED}"
    ss
      .sql(sqlText)
      .show()
    val res = ss
      .sql(sqlText)
      .first()

    assert(res.getString(0) === "22")
    assert(res.getInt(1) === 1000 + 1) //test  1-based
    assert(res.getInt(2) === 5000)
    assert(res.getString(5) === "+")
    assert(res.getAs[Array[Int]](10) === Array(567, 488))
  }

  test("Read Simple BED file") {
    val ss = SequilaSession(spark)
    val sqlText = s"SELECT * FROM ${tableNameSimpleBED}"
    ss
      .sql(sqlText)
      .show()

    val res = ss
      .sql(sqlText)
      .first()

    assert(res.getString(0) === "11")
    assert(res.getInt(1) === 1000 + 1) //test  1-based
    assert(res.getInt(2) === 5000)
    assert(res.getString(3) === null)


  }
  test("Read BED file with LIMIT") {
    val ss = SequilaSession(spark)
    ss
      .sparkContext
      .setLogLevel("INFO")
    val sqlText = s"SELECT * FROM ${tableNameBED} LIMIT 1"
    ss
      .sql(sqlText)
      .show()
  }

}
