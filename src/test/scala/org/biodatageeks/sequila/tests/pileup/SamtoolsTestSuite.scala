package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql._
import org.biodatageeks.sequila.pileup.converters.PileupConverter
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}


class SamtoolsTestSuite extends PileupTestBase {

  val splitSize = "1000000"
  val query =
    s"""
       |SELECT contig, pos_start, pos_end, ref, coverage, alts
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath')
                         """.stripMargin


  test("testing bdg-samtools single partition") {
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load(samResPath)
      .drop("quality")

    val converter = new PileupConverter(spark)
    val sam = converter.transformSamtoolsResult(df).orderBy("contig", "pos_start")

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(query).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)

    Writer.saveToFile(spark, samRes, "samRes.csv")
    Writer.saveToFile(spark, bdgRes, "bdgRes.csv")
    assertDataFrameEquals(samRes, bdgRes)
  }

  test("testing bdg-samtools multiple partitions") {
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load(samResPath).drop("quality")

    val converter = new PileupConverter(spark)
    val sam = converter.transformSamtoolsResult(df).orderBy("contig", "pos_start")

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)

    val bdgRes = ss.sql(query).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)

    Writer.saveToFile(spark, samRes, "samRes.csv")
    Writer.saveToFile(spark, bdgRes, "bdgResFullSplit.csv")
    assertDataFrameEquals(samRes, bdgRes)
  }

}
