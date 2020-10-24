package org.biodatageeks.sequila.pileup

import java.io.{OutputStreamWriter, PrintWriter}

import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}

object PileupRunner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .config("spark.driver.memory","48g")
      .config( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .config("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
//      .config("spark.kryoserializer.buffer.max", "512m")
      .enableHiveSupport()
      .getOrCreate()

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    spark.sparkContext.setLogLevel("INFO")
//    val bamPath = "/Users/mwiewior/research/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.md.bam"
        val bamPath = "/Users/mwiewior/research/data/NA12878.proper.wes.md.bam"
//  val bamPath = "/Users/mwiewior/research/data//chr1/NA12878.proper.wes.md.chr1.bam"
//    val referencePath = "/Users/mwiewior/research/data/hs37d5.fa"
        val referencePath = "/Users/mwiewior/research/data/Homo_sapiens_assembly18.fasta"
    val tableNameBAM = "reads"

    ss.sql(s"""DROP  TABLE IF  EXISTS $tableNameBAM""")
    ss.sql(s"""
              |CREATE TABLE $tableNameBAM
              |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
              |OPTIONS(path "$bamPath")
              |
      """.stripMargin)


    val query =
      s"""
         |SELECT *
         |FROM  pileup('$tableNameBAM', 'NA12878.proper', '${referencePath}', true)
       """.stripMargin

    ss
      .sqlContext
      .setConf(InternalParams.EnableInstrumentation, "false")
    Metrics.initialize(ss.sparkContext)
    val metricsListener = new MetricsListener(new RecordedMetrics())
//    ss
//      .sparkContext
//      .addSparkListener(metricsListener)
//    ss.sqlContext.setConf("spark.biodatageeks.readAligment.method", "disq")
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sparkContext.setLogLevel("WARN")
    val results = ss.sql(query)
    ss.time{
      results
//        .count()
      .write.orc("/tmp/wes_seq_pileup_2")
//        .show()
    }
    val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.close()

    ss.stop()
  }

}
