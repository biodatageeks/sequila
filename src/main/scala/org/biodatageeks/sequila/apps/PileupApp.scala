package org.biodatageeks.sequila.apps

import java.io.{OutputStreamWriter, PrintWriter}

import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}

object PileupApp extends App with SequilaApp {
  override def main(args: Array[String]): Unit = {

    val ss = createSequilaSession()

    val bamPath = "/Users/aga/NA12878.chr20.md.bam"
    val referencePath = "/Users/aga/Homo_sapiens_assembly18_chr20.fasta"
    val tableNameBAM = "reads"

    ss.sql(s"""DROP  TABLE IF  EXISTS $tableNameBAM""")
    ss.sql(s"""
              |CREATE TABLE $tableNameBAM
              |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
              |OPTIONS(path "$bamPath")
              |
      """.stripMargin)

    val queryQual =
      s"""
         |SELECT count(*)
         |FROM  pileup('$tableNameBAM', 'NA12878', '${referencePath}', true, 1)
       """.stripMargin

    val resultsQual = ss.sql(queryQual)
    ss.time{
      resultsQual.show()
    }
    ss.stop()
  }
}
