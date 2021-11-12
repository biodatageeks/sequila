package org.biodatageeks.sequila.pileup

import java.io.{OutputStreamWriter, PrintWriter}

import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}

object PileupRunner {
  def main(args: Array[String]): Unit = {
    System.setSecurityManager(null)
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.driver.memory","16g")
      .config( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .config("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
      //      .config("spark.kryoserializer.buffer.max", "1024m")
      .config("spark.driver.maxResultSize","5g")
      .config("spark.ui.showConsoleProgress", "true")
      // .config("spark.hadoop.mapred.max.split.size", "134217728")
      //      .config("spark.kryoserializer.buffer.max", "512m")
      //.enableHiveSupport()
      .getOrCreate()

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    spark.sparkContext.setLogLevel("INFO")
    //        val bamPath = "/Users/mwiewior/research/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.md.bam"
    //        val referencePath = "/Users/mwiewior/research/data/hs37d5.fa"
    val bamPath = "/data/workspace/dataset/NA12878.proper.wes.md.bam"
    val referencePath = "/data/workspace/dataset/Homo_sapiens_assembly18.fasta"
    //          val bamPath = "/Users/mwiewior/research/data/WGS/NA12878.proper.wgs.md.bam"

    //            val referencePath = "/Users/mwiewior/research/data/broad/Homo_sapiens_assembly38.fasta"
    //    val bamPath = "/Users/mwiewior/research/data/rel5-guppy-0.3.0-chunk10k.chr22.bam"
    //    val referencePath = "/Users/mwiewior/research/data/GRCh38_full_analysis_set_plus_decoy_hla.fa"
    //    val bamPath = "/Users/mwiewior/research/data/rel5-guppy-0.3.0-chunk10k.chr1"
    //    val referencePath = "/Users/mwiewior/research/data/rel5-guppy-0.3.0-chunk10k.chr1.fasta"
    val tableNameBAM = "reads"

    ss.sql(s"""DROP  TABLE IF  EXISTS $tableNameBAM""")
    ss.sql(s"""
              |CREATE TABLE $tableNameBAM
              |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
              |OPTIONS(path "$bamPath")
              |
      """.stripMargin)


    //    val query =
    //      s"""
    //         |SELECT *
    //         |FROM  pileup('$tableNameBAM', 'rel5-guppy-0.3.0-chunk10k.chr1', '${referencePath}', true)
    //       """.stripMargin

    //    val query =
    //      s"""
    //         |SELECT *
    //         |FROM  pileup('$tableNameBAM', 'NA12878.proper.wes.md', '${referencePath}', false)
    //       """.stripMargin

    val query =
      s"""
         |SELECT *
         |FROM  pileup('$tableNameBAM', 'NA12878.proper.wes.md', '${referencePath}', true)
               """.stripMargin


    //    ss.sqlContext.setConf("spark.biodatageeks.readAligment.method", "disq")
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sparkContext.setLogLevel("INFO")
    val results = ss.sql(query)
    ss.time{
      results
        .count()
      // .coalesce(128)
      //  .write.orc("/tmp/wes_seq_pileup_2")
      //        .show()
    }
    //    val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
    //    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    //    writer.close()

    ss.stop()
  }

}