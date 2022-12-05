package org.biodatageeks.sequila.ximmer

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.apps.PileupApp.createSparkSessionWithExtraStrategy
import org.biodatageeks.sequila.pileup.converters.sequila.SequilaConverter
import org.biodatageeks.sequila.utils.SystemFilesUtil.getFilename

import java.nio.file.{Files, Paths}

class PerBaseCoverage {

  /**
    * TODO kkobylin opis
    * Calculate per base coverage for each sample and save in directories in outputPath
    * Has to be calculated in independent spark session, because coverage fun spoil reads table
    */
  def calculatePerBaseCoverage(bamFiles: List[String], targetsPath: String, outputPath: String): Unit = {
    val spark = createSparkSessionWithExtraStrategy()
    val ss = SequilaSession(spark)

    ss
      .read
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(targetsPath)
      .createOrReplaceTempView("targets")

    for (bam <- bamFiles) {
      ss.sql(s"""DROP TABLE IF EXISTS reads""")
      ss.sql(
        s"""CREATE TABLE reads
           |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
           |OPTIONS(path '$bam')""".stripMargin)

      val sample = getFilename(bam)

      val coverageQuery =
        s"""
           |SELECT *
           |FROM  coverage('reads', '${sample}', 'bases')
       """.stripMargin

      val coverageDf = ss.sql(coverageQuery)
      new SequilaConverter(ss).toCommonFormat(coverageDf, true)
        .createOrReplaceTempView("reads_pb_cov")

      //Przyporządkowanie pokryć per base do przedziałów
      val joinQuery =
        """SELECT t._c0 AS Chr,
          |t._c1 AS Start,
          |t._c2 AS End,
          |r.coverage AS per_base_cov
            FROM reads_pb_cov r INNER JOIN targets t
          |ON (
          |  t._c0 = concat('chr', r.contig)
          |  AND
          |  r.pos_start >= CAST(t._c1 AS INTEGER)
          |  AND
          |  r.pos_start <= CAST(t._c2 AS INTEGER)
          |)
          |ORDER BY t._c0, CAST(t._c1 AS INTEGER) desc
          |""".stripMargin

      val targets_per_base_cov = outputPath + "/targets_per_base_cov/" + sample.toString
      Files.createDirectories(Paths.get(targets_per_base_cov))

      ss.sql(joinQuery)
        .coalesce(1)
        .write
        .mode("overwrite")
        .csv(targets_per_base_cov)
    }

    ss.stop() //TODO kkobylin takie rozwiazanie bo inaczej nawet przy ss.newSession() i drop table reads, dalej nie widac odczytow w reads
  }
}
