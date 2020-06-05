package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.biodatageeks.sequila.pileup.converters.PileupConverter
import org.biodatageeks.sequila.utils.SequilaRegister
import org.scalatest.{BeforeAndAfter, FunSuite}


class SamtoolsTestSuite extends FunSuite
  with DataFrameSuiteBase with BeforeAndAfter {

  val samResPath: String = getClass.getResource("/multichrom/mdbam/samtools.pileup").getPath
  val referencePath: String = getClass.getResource("/reference/Homo_sapiens_assembly18_chr1_chrM.small.fasta").getPath
  val bamPath: String = getClass.getResource("/multichrom/mdbam/NA12878.multichrom.md.bam").getPath
  val tableName = "reads"
  var sparkSes: SparkSession = _

  val schema: StructType = StructType(
    List(
      StructField("contig", StringType, nullable = true),
      StructField("position", IntegerType, nullable = true),
      StructField("reference", StringType, nullable = true),
      StructField("coverage", ShortType, nullable = true),
      StructField("pileup", StringType, nullable = true),
      StructField("quality", StringType, nullable = true)
    )
  )

  before {
    sparkSes = SparkSession.builder().master("local").getOrCreate()
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(
      s"""
         |CREATE TABLE $tableName
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)
    val mapToString = (map: Map[Byte, Short]) => {
      if (map == null)
        "null"
      else
        map.map({
          case (k, v) => k.toChar -> v
        }).toSeq.sortBy(_._1).mkString.replace(" -> ", ":")
    }
    spark.udf.register("mapToString", mapToString)
  }

  test("testing bdg-samtools") {
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load(samResPath).drop("quality")

    val converter = new PileupConverter(sparkSes)
    val samRes = converter.transformSamtoolsResult(df).orderBy("contig", "pos_start")

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")
    val query =
      s"""
         |SELECT contig, pos_start, pos_end, ref, coverage, alts
         |FROM  pileup('$tableName', '$referencePath')
                         """.stripMargin

    val bdgRes = ss.sql(query).orderBy("contig", "pos_start")
    val samRes2 = spark.createDataFrame(samRes.rdd, bdgRes.schema)
    assert(samRes2.schema == bdgRes.schema)
    assertDataFrameEquals(samRes2, bdgRes)

//        writeOutputFile(samRes, "samRes.csv")
//        writeOutputFile(bdgRes, "bdgRes.csv")

    assertDataFrameEquals(samRes2, bdgRes)
  }

  private def writeOutputFile(res: Dataset[Row], path: String) = {
    res
      .selectExpr("contig", "pos_start", "pos_end", "ref", "cast(coverage as int)", "mapToString(alts)")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(path)
  }
}
