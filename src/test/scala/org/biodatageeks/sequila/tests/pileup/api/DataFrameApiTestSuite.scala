package org.biodatageeks.sequila.tests.pileup.api

import org.apache.spark.sql.SequilaSession
import org.apache.spark.sql.functions.typedLit
import org.biodatageeks.sequila.tests.pileup.PileupTestBase
import org.biodatageeks.sequila.utils.SequilaRegister

class DataFrameApiTestSuite extends PileupTestBase{

  test("Coverage - DataFrame API test"){
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val testCovDS = ss
      .coverage(bamPath, referencePath)
      .toDF()
    val refCovDF = ss.sql(queryCoverage)
    assertDataFrameEquals(testCovDS, refCovDF)
  }

  test("Pileup - without quals - DataFrame API test"){
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val testPileup = ss
      .pileup(bamPath, referencePath, quals = false)
      .toDF()
    val refPileup = ss.sql(queryPileupWithoutQual)
      .withColumn("quals", typedLit[Option[Map[Byte, Array[Short]]]](None) )
    assertDataFrameEquals(testPileup, refPileup)
  }

  test("Pileup - with quals - DataFrame API test"){
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val testPileup = ss
      .pileup(bamPath, referencePath)
      .toDF()
    val refPileup = ss.sql(queryPileupWithQual)
    assertDataFrameEquals(testPileup, refPileup)
  }

}
