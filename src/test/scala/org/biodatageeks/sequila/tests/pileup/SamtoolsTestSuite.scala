package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql._
import org.biodatageeks.sequila.pileup.{PileupReader, PileupWriter}
import org.biodatageeks.sequila.pileup.converters.samtools.{SamtoolsConverter, SamtoolsSchema}
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}


class SamtoolsTestSuite extends PileupTestBase {

  val splitSize = "1000000"
  val qualAgg = "quals_agg"
  val query =
    s"""
       |SELECT contig, pos_start, pos_end, ref, coverage, alts
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath')
                         """.stripMargin

  val queryQual =
    s"""
       |SELECT contig, pos_start, pos_end, ref, coverage, alts, quals_to_map(${Columns.QUALS}) as $qualAgg
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true)
                         """.stripMargin

  val queryQualSingleChrom =
    s"""
       |SELECT contig, pos_start, pos_end, ref, coverage, alts, quals_to_map(${Columns.QUALS}) as $qualAgg
       |FROM  pileup('$tableNameSingleChrom', '${sampleIdSingleChrom}', '$singleChromRefPath', true)
                         """.stripMargin

  test("SINGLE CHROM: alts,quals: one partition") {
    //    val df = PileupReader.load(spark, samResPath, SamtoolsSchema.schema, delimiter = "\t", quote = "\u0000")
    //
    //    val converter = new SamtoolsConverter(spark)
    //    val sam = converter
    //      .transformToBlocks(df, caseSensitive = true)
    //      .orderBy("contig", "pos_start")

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(queryQualSingleChrom).orderBy("contig", "pos_start")
    //val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)
    bdgRes.show()

    // assertDataFrameEquals(samRes, bdgRes)
  }



  test("MULTI CHROM: alts: one partition") {
    val df = PileupReader.load(spark, samResPath, SamtoolsSchema.schema, delimiter = "\t", quote = "\u0000")

    val converter = new SamtoolsConverter(spark)
    val sam = converter
      .transformToBlocks(df, caseSensitive = true)
      .select(Columns.CONTIG, Columns.START, Columns.END,Columns.REF,  Columns.COVERAGE, Columns.ALTS)
      .orderBy("contig", "pos_start")

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(query).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)

    //    PileupWriter.save(samRes, "samRes.csv")
    //    PileupWriter.save(spark, bdgRes, "bdgRes.csv")
    assertDataFrameEquals(samRes, bdgRes)
  }

  test("MULTI CHROM: alts: many partitions") {
    val df = PileupReader.load(spark, samResPath, SamtoolsSchema.schema, delimiter = "\t", quote = "\u0000")

    val converter = new SamtoolsConverter(spark)
    val sam = converter
      .transformToBlocks(df, caseSensitive = true)
      .select(Columns.CONTIG, Columns.START, Columns.END,Columns.REF, Columns.COVERAGE,Columns.ALTS)
      .orderBy("contig", "pos_start")

    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(query).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)

    assertDataFrameEquals(samRes, bdgRes)
  }


  test("MULTI CHROM: alts,quals: one partition") {
    val df = PileupReader.load(spark, samResPath, SamtoolsSchema.schema, delimiter = "\t", quote = "\u0000")

    val converter = new SamtoolsConverter(spark)
    val sam = converter
      .transformToBlocks(df, caseSensitive = true)
      .orderBy("contig", "pos_start")

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(queryQual).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)

    assertDataFrameEquals(samRes, bdgRes)
  }


  test("MULTI CHROM: alts,quals: many partitions ") {
    val df = PileupReader.load(spark, samResPath, SamtoolsSchema.schema, delimiter = "\t", quote = "\u0000")

    val converter = new SamtoolsConverter(spark)
    val sam = converter
      .transformToBlocks(df, caseSensitive = true)
      .orderBy("contig", "pos_start")

    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(queryQual).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)

    assertDataFrameEquals(samRes, bdgRes)
  }

}
