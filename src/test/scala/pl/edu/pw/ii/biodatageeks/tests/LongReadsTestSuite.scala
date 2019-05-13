package pl.edu.pw.ii.biodatageeks.tests

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.bdgenomics.utils.instrumentation.Metrics
import org.biodatageeks.utils.{BDGInternalParams, SequilaRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class LongReadsTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext {

  val bamPath = getClass.getResource("/nanopore_guppy_slice.bam").getPath
  val splitSize = 30000
  val tableNameBAM = "reads"

  before {

    System.setSecurityManager(null)
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)

  }
//  test("BAM - Nanopore with guppy basecaller") {
//
//    val session: SparkSession = SequilaSession(spark)
//    SequilaRegister.register(session)
//    session
//      .sparkContext
//      .setLogLevel("WARN")
//    val bdg = session.sql(s"SELECT * FROM ${tableNameBAM}")
//    assert(bdg.count() === 150)
//  }

  test("BAM - coverage - Nanopore with guppy basecaller") {
    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, (splitSize*10).toString)
    val session2: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session2)
    val query = s"SELECT contigName, start, coverage FROM bdg_coverage('${tableNameBAM}','nanopore_guppy_slice','bases') order by contigName,start,end"

    val covMultiPartitionDF = session2.sql(query)

    //covMultiPartitionDF.coalesce(1).write.mode("overwrite").option("delimiter", "\t").csv("/Users/aga/workplace/multiPart.csv")
    assert(covMultiPartitionDF.count() == 45620) // total count check 45620<---> 45842

    assert ( covMultiPartitionDF.filter("coverage== 0" ).count==0)


    assert(covMultiPartitionDF.where("contigName='chr21' and start == 5010515").first().getShort(2) == 1) // value check [first element]
    assert(covMultiPartitionDF.where("contigName='chr21' and start == 5022667").first().getShort(2) == 15) // value check [partition boundary]
    assert(covMultiPartitionDF.where("contigName='chr21' and start == 5036398").first().getShort(2) == 14) // value check [partition boundary]
    assert(covMultiPartitionDF.where("contigName='chr21' and start == 5056356").first().getShort(2) == 1) // value check [last element]

  }

}