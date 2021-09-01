package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.sequila.utils.InternalParams
import org.scalatest.{BeforeAndAfter, FunSuite}


class PileupTestBase extends FunSuite
  with DataFrameSuiteBase
  with BeforeAndAfter
  with SharedSparkContext{

  val sampleId = "NA12878.multichrom.md"
  val sampleIdSingleChrom="NA12878.chr20.short.md"
  val samResPath: String = getClass.getResource("/multichrom/mdbam/samtools_x.pileup").getPath
  val samResSinglePath: String = getClass.getResource("/singlechrom/samtools_chr20_x.pileup").getPath
  val referencePath: String = getClass.getResource("/reference/Homo_sapiens_assembly18_chr1_chrM.small.fasta").getPath
  val singleChromRefPath: String = getClass.getResource("/reference/Homo_sapiens_assembly18_chr20.fasta").getPath
  val bamPath: String = getClass.getResource(s"/multichrom/mdbam/${sampleId}.bam").getPath
  val singleChromBamPath: String = getClass.getResource(s"/singlechrom/${sampleIdSingleChrom}.bam").getPath
  val cramPath : String = getClass.getResource(s"/multichrom/mdcram/${sampleId}.cram").getPath
  val tableName = "reads_bam"
  val tableNameSingleChrom="reads_single_chrom"
  val tableNameCRAM = "reads_cram"

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

    spark.sql(s"DROP TABLE IF EXISTS $tableNameSingleChrom")
    spark.sql(
      s"""
         |CREATE TABLE $tableNameSingleChrom
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$singleChromBamPath")
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
