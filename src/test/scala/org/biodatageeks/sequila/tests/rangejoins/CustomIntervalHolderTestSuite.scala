package org.biodatageeks.sequila.tests.rangejoins


import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeRedBlack
import org.biodatageeks.sequila.rangejoins.methods.base.BaseNode
import org.biodatageeks.sequila.tests.base.IntervalJoinBaseTestSuite
import org.biodatageeks.sequila.utils.InternalParams

class DummyHolder[V] extends IntervalTreeRedBlack[V] {

}

class CustomIntervalHolderTestSuite  extends IntervalJoinBaseTestSuite {
  test("Check spark.biodatageeks.rangejoin.intervalHolderClass"){

    spark
      .sqlContext.setConf(InternalParams.useJoinOrder, "true")
    spark
      .sqlContext.setConf(InternalParams.intervalHolderClass,
      Class.forName("org.biodatageeks.sequila.tests.rangejoins.DummyHolder").getName)
    val query =
      s"""
         |SELECT snp.*,ref.* FROM snp JOIN ref
         |ON (ref.chr=snp.chr AND snp.end>=ref.start AND snp.start<=ref.end)
       """.stripMargin
    assert(spark.sql(query).count === 616404L)
    println(spark
      .sql(query)
      .queryExecution
      .executedPlan
      .prettyJson
      .contains(""""intervalHolderClassName" : "org.biodatageeks.sequila.tests.rangejoins.DummyHolder""""))

  }
}
