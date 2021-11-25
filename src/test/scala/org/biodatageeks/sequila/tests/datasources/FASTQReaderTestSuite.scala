package org.biodatageeks.sequila.tests.datasources

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.base.FASTQBaseTestSuite

class FASTQReaderTestSuite extends FASTQBaseTestSuite with SharedSparkContext {

  test("Read FASTQ file"){

    val ss = SequilaSession(spark)
    val sqlText =  s"SELECT * FROM ${tableNameFASTQ}"
    ss
      .sql(sqlText)
      .show()
    val res = ss
      .sql(sqlText)
      .first()
   assert(res.getString(0) === "NA12988")
   assert(res.getBoolean(8) === false)
   assert(res.getString(11) == "GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT")
   assert(res.getString(12) == "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65")
  }

}
