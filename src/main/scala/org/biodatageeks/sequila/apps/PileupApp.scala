package org.biodatageeks.sequila.apps


object PileupApp extends App with SequilaApp {
  override def main(args: Array[String]): Unit = {

    val ss = createSequilaSession()

    val bamPath = "/Users/aga/workplace/sequila/src/test/resources/multichrom/mdbam/NA12878.multichrom.md.bam"
    val referencePath = "/Users/aga/workplace/data/Homo_sapiens_assembly18_chr20.fasta"
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
         |SELECT *
         |FROM  pileup('$tableNameBAM', 'NA12878.chr20', '${referencePath}', true, 1)
       """.stripMargin

    val resultsQual = ss.sql(queryQual)
    ss.time{
      resultsQual.show()
    }
    ss.stop()
  }
}
