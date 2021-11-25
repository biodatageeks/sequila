package org.biodatageeks.sequila.apps

import org.apache.spark.sql.{SequilaSession, SparkSession}


object CovRun {

  def main(args: Array[String]): Unit = {
    //val bamPath = "/Users/marek/Downloads/data/NA12878.ga2.exome.maq.recal.bam"
    val bamPath = "/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam"
    val tableNameBAM = "reads"

    val spark = SparkSession
      .builder()
      .config("spark.driver.memory", "8g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[4]")
      .getOrCreate()
    val ss = new SequilaSession(spark)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","false")
    ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","true")
    ss.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    ss.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)

    spark.time {
      ss.sql(
        s"""SELECT * FROM coverage('${tableNameBAM}','NA12878.chr','bdg')
         """.stripMargin).count
    }

//    spark.time {
//      ss.sql(
//        s"""SELECT * FROM coverage('${tableNameBAM}','NA12878.chr','bdg')
//          | WHERE start >=   25109993
//         """.stripMargin).show(20)
//    }

//    spark.time {
//      ss.sql(
//        s"""SELECT * FROM coverage('${tableNameBAM}','NA12878.chr','mosdepth')
//           | WHERE start >=   25109993
//         """.stripMargin).show(20)
//    }
  }

}
//25109928