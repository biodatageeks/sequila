package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.biodatageeks.sequila.pileup.{PileupReader, PileupWriter}
import org.biodatageeks.sequila.pileup.converters.samtools.{SamtoolsConverter, SamtoolsSchema}
import org.biodatageeks.sequila.pileup.converters.sequila.SequilaConverter
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}


class SamtoolsTestSuite extends PileupTestBase {

  val splitSize = "1000000"
  val qualAgg = "quals_agg"
  val queryCov =
    s"""
       |SELECT contig, pos_start, pos_end, ref, coverage
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath')
                         """.stripMargin

  val queryQual =
    s"""
       |SELECT contig, pos_start, pos_end, ref, coverage, altmap_to_str(alts_to_char(${Columns.ALTS})) as ${Columns.ALTS} , qualsmap_to_str(to_charmap(${Columns.QUALS})) as ${Columns.QUALS}
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true, true)
                         """.stripMargin

  private def loadSam(schema: StructType): DataFrame = {
    val samtoolsRaw = PileupReader.load(spark, samResPath, SamtoolsSchema.schema, delimiter = "\t", quote = "\u0000")

    val converter = new SamtoolsConverter(spark)
    val pileup = converter
      .toCommonFormat(samtoolsRaw, caseSensitive = true)
      .select(Columns.CONTIG, Columns.START, Columns.END,Columns.REF,  Columns.COVERAGE, Columns.ALTS, Columns.QUALS)
      .orderBy("contig", "pos_start")
    spark.createDataFrame(pileup.rdd, schema)
  }

  private def calculateSequilaPileup(query: String, splitSize: Option[String]): DataFrame = {
    if (splitSize.isDefined)
      spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize.get)

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val sequilaRaw = ss.sql(query).orderBy("contig", "pos_start")

    val converter = new SequilaConverter(spark)
    converter.toCommonFormat(sequilaRaw, true)
  }


  test("MULTI CHROM one partition") {

    val sequilaPileup = calculateSequilaPileup(queryQual, None)
    val samPileup = loadSam(sequilaPileup.schema)
//        PileupWriter.save(samPileup, "samRes.csv")
//        PileupWriter.save(sequilaPileup, "sequilaPileup.csv")
    assertDataFrameEquals(samPileup, sequilaPileup)
  }

  test("MULTI CHROM: many partitions") {

    val sequilaPileup = calculateSequilaPileup(queryQual, Some(splitSize))
    val samPileup = loadSam(sequilaPileup.schema)
    assertDataFrameEquals(samPileup, sequilaPileup)
  }

  test("COV only") {

    val sequilaPileup = calculateSequilaPileup(queryCov, Some(splitSize)).select("contig", "pos_start", "pos_end", "coverage")
    val samPileup = loadSam(sequilaPileup.schema).select("contig", "pos_start", "pos_end", "coverage")
    assertDataFrameEquals(samPileup, sequilaPileup)
  }

}
