package org.biodatageeks.sequila.tests.pileup.converters

import org.biodatageeks.sequila.pileup.converters.SamtoolsConverter
import org.biodatageeks.sequila.pileup.converters.samtools.{SamtoolsConverter, SamtoolsSchema}
import org.biodatageeks.sequila.tests.pileup.PileupTestBase

class SamToCommonFormatTestSuite extends PileupTestBase {

  val samPath: String = getClass.getResource("/pileup/samtools.csv").getPath

  test("samtools to common format") {
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(SamtoolsSchema.schema)
      .load(samPath)

    val converter = new SamtoolsConverter(spark)
    val commonDf = converter.transformToCommonFormat(df, false)
    commonDf.show(20)
  }
}
