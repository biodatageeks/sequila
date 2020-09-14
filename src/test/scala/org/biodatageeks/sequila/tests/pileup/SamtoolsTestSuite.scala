package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql._
import org.biodatageeks.sequila.pileup.converters.PileupConverter
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


  test("alts: one partition") {
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load(samResPath)

    val converter = new PileupConverter(spark)
    val sam = converter
      .transformSamtoolsResult(df)
      .select(Columns.CONTIG, Columns.START, Columns.END,Columns.REF,  Columns.COVERAGE,Columns.ALTS)
      .orderBy("contig", "pos_start")

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(query).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)

    Writer.saveToFile(spark, samRes, "samRes.csv")
    Writer.saveToFile(spark, bdgRes, "bdgRes.csv")
    assertDataFrameEquals(samRes, bdgRes)
  }

  test("alts: many partitions") {
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load(samResPath)

    val converter = new PileupConverter(spark)
    val sam = converter
      .transformSamtoolsResult(df)
      .select(Columns.CONTIG, Columns.START, Columns.END,Columns.REF, Columns.COVERAGE,Columns.ALTS)
      .orderBy("contig", "pos_start")

    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)


    val bdgRes = ss.sql(query).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)

//    Writer.saveToFile(spark, samRes, "samRes.csv")
//    Writer.saveToFile(spark, bdgRes, "bdgResFullSplit.csv")


    assertDataFrameEquals(samRes, bdgRes)
  }


  test("alts,quals: one partition") {
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load(samResPath)

    val converter = new PileupConverter(spark)
    val sam = converter
      .transformSamtoolsResult(df)
      .orderBy("contig", "pos_start")

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(queryQual).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)


//    bdgRes.where("pos_start = 7856").show(truncate = false)

    assertDataFrameEquals(samRes, bdgRes)
  }


  test("alts,quals: many partitions ") {
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load(samResPath)

    val converter = new PileupConverter(spark)
    val sam = converter
      .transformSamtoolsResult(df)
      .orderBy("contig", "pos_start")

    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(queryQual).orderBy("contig", "pos_start")
    val samRes = spark.createDataFrame(sam.rdd, bdgRes.schema)

    assertDataFrameEquals(samRes, bdgRes)
  }

}
