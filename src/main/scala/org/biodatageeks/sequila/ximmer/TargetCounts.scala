/**
  * Created by Krzysztof Kobyliński
  */
package org.biodatageeks.sequila.ximmer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.utils.SystemFilesUtil._

import scala.collection.mutable

class TargetCounts {

  def calculateTargetCounts(ss: SparkSession, targetsPath: String, bamFiles: List[String], saveBamInfo: Boolean): mutable.SortedMap[String, (DataFrame, Long)] = {
    val resultMap = mutable.SortedMap[String, (DataFrame, Long)]()

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

      val intervalJoinQuery =
        """SELECT t._c0 AS Chr,
          |   t._c1 AS Start,
          |   t._c2 AS End,
          |   SUM(CASE WHEN
          |       r.flag & 1604 == 64 AND --!read unmapped and first in pair and !read fails and !read duplicate
          |       r.mapq > 20 AND
          |       r.pos_end >= CAST(t._c1 AS INTEGER) THEN 1 ELSE 0 END
          |   ) AS codex_cov,
          |   SUM(CASE WHEN
          |       r.mapq > 1 AND
          |       r.pos_start >= CAST(t._c1 AS INTEGER) AND
          |       r.pos_start < CAST(t._c2 AS INTEGER) THEN 1 ELSE 0 END
          |   ) AS cnmops_cov,
          |   SUM(CASE WHEN
          |       r.flag & 1295 == 3 AND -- read paired and mapped in proper pair and !unmapped and !mate unmapped and !secondary alignment and !duplicate
          |       r.mapq > 20 AND
          |       r.pos_start + r.tlen > CAST(t._c1 AS INTEGER) AND
          |       r.pos_start < CAST(t._c2 AS INTEGER) THEN 1 ELSE 0 END
          |   ) AS ed_cov,
          |   SUM(CASE WHEN
          |       r.pos_start >= CAST(t._c1 AS INTEGER) THEN 1 ELSE 0 END
          |   ) AS conifer_cov
          |FROM reads r INNER JOIN targets t
          |ON (
          |  t._c0 = concat('chr', r.contig)
          |  AND
          |  r.pos_start <= CAST(t._c2 AS INTEGER)
          |  AND
          |  r.pos_end + r.tlen >= CAST(t._c1 AS INTEGER) --Warunek dodatkowy zeby ograniczyc ilosc rekordow w joinie
          |)
          |WHERE r.contig is not null
          |   AND r.pos_start < r.pos_end
          |GROUP BY t._c0, t._c1, t._c2
          |""".stripMargin

      ss.sql(intervalJoinQuery)
        .createOrReplaceTempView("result");

      val includeAllTargetsQuery =
        """SELECT t._c0 AS Chr,
          |       t._c1 AS Start,
          |       t._c2 AS End,
          |       CASE WHEN codex_cov IS NULL THEN 0 ELSE codex_cov END AS codex_cov,
          |       CASE WHEN cnmops_cov IS NULL THEN 0 ELSE cnmops_cov END AS cnmops_cov,
          |       CASE WHEN ed_cov IS NULL THEN 0 ELSE ed_cov END AS ed_cov,
          |       CASE WHEN conifer_cov IS NULL THEN 0 ELSE conifer_cov END AS conifer_cov
          |FROM targets t
          |       LEFT JOIN result r on r.chr = t._c0 AND r.start = t._c1 AND r.end = t._c2
          |ORDER BY chr, CAST(start AS INTEGER)
          |""".stripMargin

      val resultDF = ss.sql(includeAllTargetsQuery).cache()
      resultDF.createOrReplaceTempView("result_all_targets")

      var readsNr = 0L
      if (saveBamInfo) {
        val startTime = System.currentTimeMillis()
        val countQuery = "Select sum(conifer_cov) from result_all_targets"
        readsNr = ss.sql(countQuery)
          .first()
          .getLong(0)
        val endTimeEnd = System.currentTimeMillis()

        println("Select count time: " + (endTimeEnd - startTime) / 1000)
      }

      resultMap += (sample -> (resultDF, readsNr))
    }
    return resultMap
  }

}
