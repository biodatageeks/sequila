package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.bdgenomics.utils.instrumentation.{MetricsListener, RecordedMetrics}
import org.biodatageeks.utils.SequilaRegister
import org.scalatest.{BeforeAndAfter, FunSuite}

class BAMBDGReaderTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{

  val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
  //val bamPath = "/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam"

  val tableNameBAM = "reads"

  before{
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)
  }

  test("Test point query predicate pushdown"){
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","false")
//    val query =  """
//                   |SELECT * FROM reads WHERE contigName='20' AND start=59993
//                 """.stripMargin
    val query =  """
                       |SELECT * FROM reads WHERE contigName='chr1' AND start=20138
                     """.stripMargin
    val withoutPPDF = ss.sql(query).collect()

    ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","true")
    val withPPDF = ss.sql(query)
    assertDataFrameEquals(ss.createDataFrame(ss.sparkContext.parallelize(withoutPPDF),withPPDF.schema),withPPDF)
    spark.time {
      ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown", "false")
      ss.sql(query).count
    }

  }

  test("Test interval query predicate pushdown"){
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","false")
    val query =  """
                   |SELECT * FROM reads WHERE contigName='chr1' AND start >= 1996 AND end <= 2071
                 """.stripMargin
    val withoutPPDF = ss.sql(query).collect()

    ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","true")
    val withPPDF = ss.sql(query)
    assertDataFrameEquals(ss.createDataFrame(ss.sparkContext.parallelize(withoutPPDF),withPPDF.schema),withPPDF)
    spark.time {
      ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown", "false")
      ss.sql(query).show(50)
    }

  }
}

