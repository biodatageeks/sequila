package org.biodatageeks.sequila.ximmer

import org.apache.spark.sql.{DataFrame, SequilaSession}
import org.biodatageeks.sequila.apps.PileupApp.createSparkSessionWithExtraStrategy
import org.biodatageeks.sequila.pileup.converters.sequila.SequilaConverter
import org.biodatageeks.sequila.utils.InternalParams
import org.biodatageeks.sequila.utils.SystemFilesUtil.getFilename

import scala.collection.mutable

class PerBaseCoverage {

  /**
    * TODO kkobylin opis
    * Calculate per base coverage for each sample and save in directories in outputPath
    * Has to be calculated in independent spark session, because coverage fun spoil reads table
    */
  def calculatePerBaseCoverage(bamFiles: List[String], targetsPath: String): mutable.Map[String, (DataFrame, DataFrame)] = {
    val spark = createSparkSessionWithExtraStrategy()
    val ss = SequilaSession(spark)
    ss.sqlContext.setConf(InternalParams.filterReadsByFlag, "2316")
    ss.sqlContext.setConf(InternalParams.filterReadsByMQ, "1")

    ss
      .read
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(targetsPath)
      .createOrReplaceTempView("targets")

    val resultMap = mutable.SortedMap[String, (DataFrame, DataFrame)]()

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
      val perBaseCoverage = new SequilaConverter(ss).convertToPerBaseOutput(coverageDf)
      perBaseCoverage.createOrReplaceTempView("reads_pb_cov")

      val intervalQuery =
        """SELECT r.*
          |FROM reads_pb_cov r INNER JOIN targets t
          |ON (
          |  t._c0 = concat('chr', r._1)
          |  AND
          |  r._2 >= CAST(t._c1 AS INTEGER) - 1
          |  AND
          |  r._2 <= CAST(t._c2 AS INTEGER) - 1
          |)
       """.stripMargin

      val narrowPerBaseCoverage = ss.sql(intervalQuery)
      narrowPerBaseCoverage.createOrReplaceTempView("narrow_reads_pb_cov")

      val meanCoverageQuery =
      """SELECT t._c0 AS Chr,
        |t._c1 AS Start,
        |t._c2 AS End,
        |sum(r._5) / (CAST(t._c2 AS INTEGER) - CAST(t._c1 AS INTEGER) + 1) AS mean_cov
                  FROM narrow_reads_pb_cov r INNER JOIN targets t
        |ON (
        |  t._c0 = concat('chr', r._1)
        |  AND
        |  r._2 >= CAST(t._c1 AS INTEGER) - 1
        |  AND
        |  r._2 <= CAST(t._c2 AS INTEGER) - 1
        |)
        |GROUP BY t._c0, t._c1, t._c2
        |ORDER BY t._c0, CAST(t._c1 AS INTEGER)
        |""".stripMargin

      val meanCoverage = ss.sql(meanCoverageQuery)
      resultMap += (sample -> (meanCoverage, narrowPerBaseCoverage))
    }

    return resultMap
  }
}
