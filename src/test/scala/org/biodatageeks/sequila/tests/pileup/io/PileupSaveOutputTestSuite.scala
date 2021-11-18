package org.biodatageeks.sequila.tests.pileup.io

import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.tests.pileup.PileupTestBase
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}
import org.scalatest.BeforeAndAfterAll

import java.io.File
import scala.reflect.io.Directory

class PileupSaveOutputTestSuite
  extends PileupTestBase
    with BeforeAndAfterAll
    with RDDComparisons{

  val seed = randomString(10)
  val baseOutputPath = s"/tmp/sequila_${seed}/"
  val coveragePath = s"$baseOutputPath/coverage/"
  val pileupPath = s"$baseOutputPath/pileup/"


  test("Parquet save"){
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val parquetCoveragePath = s"$coveragePath/parquet/"
    val parquetPileupPath = s"$pileupPath/parquet/"
    val covRefDF = ss.sql(queryCoverage) //using rdd to not fight with nullability/schema just byte equality
    covRefDF
      .write
      .parquet(parquetCoveragePath)
    val covTestDF = ss
      .read
      .parquet(parquetCoveragePath)
    assertRDDEquals(covRefDF.rdd, covTestDF.rdd)


    val pileupRefDF = ss.sql(queryPileupWithQual) //using rdd to not fight with nullability/schema just byte equality
    pileupRefDF
      .write
      .parquet(parquetPileupPath)
    val pileupTestDF = ss
      .read
      .parquet(parquetPileupPath)
    assertRDDEquals(pileupRefDF.rdd, pileupTestDF.rdd)
  }

  test("ORC save") {

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val orcCoveragePath = s"$coveragePath/orc/"
    val orcPileupPath = s"$pileupPath/orc/"
    val covRefDF = ss.sql(queryCoverage) //using rdd to not fight with nullability/schema just byte equality
    covRefDF
      .write
      .orc(orcCoveragePath)
    val covTestDF = ss
      .read
      .orc(orcCoveragePath)
    assertRDDEquals(covRefDF.rdd, covTestDF.rdd)


    val pileupRefDF = ss.sql(queryPileupWithQual) //using rdd to not fight with nullability/schema just byte equality
    pileupRefDF
      .write
      .orc(orcPileupPath)
    val pileupTestDF = ss
      .read
      .orc(orcPileupPath)
    assertRDDEquals(pileupRefDF.rdd, pileupTestDF.rdd)

  }

  test("ORC save - vectorized"){
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss
      .sqlContext
      .setConf(InternalParams.useVectorizedOrcWriter, "true")
    val orcCoveragePath = s"$coveragePath/orc/"
    cleanup(orcCoveragePath)
    var covRefDF = ss.sql(queryCoverage) //using rdd to not fight with nullability/schema just byte equality
    covRefDF
      .write
      .orc(orcCoveragePath)
    val covTestDF = ss
      .read
      .orc(orcCoveragePath)
    assert(covRefDF.count === 0) //should be 0 since we are bypassing DataFrame API
    ss
      .sqlContext
      .setConf(InternalParams.useVectorizedOrcWriter, "false")
    covRefDF = ss.sql(queryCoverage)
    assert(covRefDF.count === covTestDF.count())
    assertRDDEquals(covRefDF.rdd, covTestDF.rdd)
  }

  override def afterAll {
    cleanup(baseOutputPath)
  }

}