package org.biodatageeks.sequila.tests.rangejoins.iitii

import org.biodatageeks.sequila.tests.base.IntervalJoinBaseTestSuite
import org.biodatageeks.sequila.tests.rangejoins.BamBedChecker
import org.biodatageeks.sequila.utils.InternalParams

class IITIITestSuite extends IntervalJoinBaseTestSuite {

  val classHolder : String = "org.biodatageeks.sequila.rangejoins.exp.iitii.ImplicitIntervalTreeWithInterpolationIndex"

  test("Check spark.biodatageeks.sequila.rangejoins.exp.iitii.ImplicitIntervalTreeWithInterpolationIndex1") {
    spark
      .sqlContext.setConf(InternalParams.useJoinOrder, "true")
    spark
      .sqlContext.setConf(InternalParams.intervalHolderClass,
      Class.forName(classHolder).getName)
    val query =
      s"""
         |SELECT snp.*,ref.* FROM snp JOIN ref
         |ON (ref.chr=snp.chr AND snp.end>=ref.start AND snp.start<=ref.end)
       """.stripMargin
    assert(spark.sql(query).count === 616404L)
    assert(spark
      .sql(query)
      .queryExecution
      .executedPlan
      .prettyJson
      .contains(s""""intervalHolderClassName" : "$classHolder""""))
  }
  test("Check spark.biodatageeks.sequila.rangejoins.exp.iitii.ImplicitIntervalTreeWithInterpolationIndex2") {
    assert(BamBedChecker.overlapsCount(spark, "bed/fBrain-DS14718.bed", classHolder) == 529)
  }
}
