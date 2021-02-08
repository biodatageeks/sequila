package org.biodatageeks.sequila.apps

import java.io.{OutputStreamWriter, PrintWriter}

import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}

object PileupApp extends App with SequilaApp {
  override def main(args: Array[String]): Unit = {

    System.setProperty("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
    val ss = createSequilaSession()

    val bamPath = "/Users/aga/NA12878.chr20.md.bam"
    val referencePath = "/Users/aga/Homo_sapiens_assembly18_chr20.fasta"

    //    val bamPath = "/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.md.bam"
    //    val referencePath = "/Users/marek/data/hs37d5.fa"

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
         |SELECT count(*)
         |FROM  pileup('$tableNameBAM', 'NA12878', '${referencePath}')
       """.stripMargin

    val queryQual =
      s"""
         |SELECT count(*)
         |FROM  pileup('$tableNameBAM', 'NA12878', '${referencePath}', true, 1)
       """.stripMargin
//    ss
//      .sqlContext
//      .setConf(InternalParams.EnableInstrumentation, "true")
//    Metrics.initialize(ss.sparkContext)
//    val metricsListener = new MetricsListener(new RecordedMetrics())
//    ss
//      .sparkContext
//      .addSparkListener(metricsListener)
//    val results = ss.sql(query)
//    ss.time{
//      results.show()
//    }
//    val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
//    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
//    writer.close()

    val resultsQual = ss.sql(queryQual)
    ss.time{
      resultsQual.show()
    }
//    val writerQual = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
//    Metrics.print(writerQual, Some(metricsListener.metrics.sparkMetrics.stageTimes))
//    writerQual.close()

    ss.stop()
  }
}
