package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.sequila.utils.InternalParams
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter}

import java.io.File
import scala.reflect.io.Directory


class PileupTestBase extends AnyFunSuite
  with DataFrameSuiteBase
  with BeforeAndAfter
  with SharedSparkContext{

  System.setSecurityManager(null)

  val sampleId = "NA12878.multichrom.md"
  val samResPath: String = getClass.getResource("/multichrom/mdbam/samtools_x.pileup").getPath
  val referencePath: String = getClass.getResource("/reference/Homo_sapiens_assembly18_chr1_chrM.small.fasta").getPath
  val bamPath: String = getClass.getResource(s"/multichrom/mdbam/${sampleId}.bam").getPath
  val cramPath : String = getClass.getResource(s"/multichrom/mdcram/${sampleId}.cram").getPath
  val tableName = "reads_bam"
  val tableNameCRAM = "reads_cram"

  def cleanup(dir: String) = {
    val directory = new Directory(new File(dir))
    directory.deleteRecursively()
  }

  val queryCoverage =
    s"""
       |SELECT *
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', false, false)
                         """.stripMargin

  val queryPileupWithQual =
    s"""
       |SELECT *
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true, true)
                         """.stripMargin

  val queryPileupWithoutQual =
    s"""
       |SELECT *
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true, false)
                         """.stripMargin


  def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString

  before {
    System.setProperty("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
    spark.sqlContext.setConf(InternalParams.SerializationMode, StorageLevel.DISK_ONLY.toString())

    spark
      .conf.set("spark.sql.shuffle.partitions",1) //FIXME: In order to get orderBy in Samtools tests working - related to exchange partitions stage
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(
      s"""
         |CREATE TABLE $tableName
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS $tableNameCRAM")
    spark.sql(
      s"""
         |CREATE TABLE $tableNameCRAM
         |USING org.biodatageeks.sequila.datasources.BAM.CRAMDataSource
         |OPTIONS(path "$cramPath", refPath "$referencePath" )
         |
      """.stripMargin)
  }
}
