package org.biodatageeks.sequila.ximmer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.pileup.converters.sequila.SequilaConverter
import org.biodatageeks.sequila.utils.InternalParams
import org.biodatageeks.sequila.utils.SystemFilesUtil.getFilename

import scala.collection.mutable

class PerBaseCoverage {

  def calculatePerBaseCoverage(ss: SparkSession, bamFiles: List[String], targetsPath: String): mutable.Map[String, (DataFrame, DataFrame, Short)] = {
    ss.sqlContext.setConf(InternalParams.filterReadsByFlag, "2316")
    ss.sqlContext.setConf(InternalParams.filterReadsByMQ, "1")

    ss
      .read
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(targetsPath)
      .createOrReplaceTempView("targets")

    val resultMap = mutable.SortedMap[String, (DataFrame, DataFrame, Short)]()

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
        """SELECT r.*, t._c0 as chr , t._c1 as start, t._c2 as end
          |FROM reads_pb_cov r INNER JOIN targets t
          |ON (
          |  t._c0 = concat('chr', r._1)
          |  AND
          |  r._2 >= CAST(t._c1 AS INTEGER)
          |  AND
          |  r._2 <= CAST(t._c2 AS INTEGER) - 2
          |)
       """.stripMargin

      val narrowPerBaseCoverage = ss.sql(intervalQuery)
      narrowPerBaseCoverage.createOrReplaceTempView("narrow_reads_pb_cov")

      val meanCoverageQuery =
      """SELECT chr, start, end,
        |sum(r._5) / (CAST(end AS INTEGER) - CAST(start AS INTEGER) - 1) AS mean_cov
        |FROM narrow_reads_pb_cov r
        |GROUP BY chr, start, end
        |""".stripMargin

      val meanCoverage = ss.sql(meanCoverageQuery)
      meanCoverage.createOrReplaceTempView("mean_coverage")

      val includeAllTargetsQuery =
        """SELECT t._c0 AS Chr,
          |       t._c1 AS Start,
          |       t._c2 AS End,
          |       CASE WHEN mean_cov IS NULL THEN 0 ELSE mean_cov END
          |FROM targets t
          |       LEFT JOIN mean_coverage r on r.chr = t._c0 AND r.start = t._c1 AND r.end = t._c2
          |ORDER BY chr, CAST(start AS INTEGER)
          |""".stripMargin

      val allMeanCoverage = ss.sql(includeAllTargetsQuery)

      val medianQuery =
        """
          |Select percentile_approx(r._5, 0.5)
          |FROM narrow_reads_pb_cov r
          |""".stripMargin

      val median = ss.sql(medianQuery).first().getShort(0)

      resultMap += (sample -> (allMeanCoverage, narrowPerBaseCoverage, median))
    }

    return resultMap
  }
}
