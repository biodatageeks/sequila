package org.biodatageeks.sequila.tests.pileup.converters

import org.biodatageeks.sequila.pileup.converters.SamToCommonFormatConverter
import org.biodatageeks.sequila.tests.pileup.PileupTestBase

class SamToCommonFormatTestSuite extends PileupTestBase {

  val samPath: String = getClass.getResource("/pileup/samtools.csv").getPath

  test("alts: one partition") {
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load(samPath)

    val converter = new SamToCommonFormatConverter(spark)
  }



}
