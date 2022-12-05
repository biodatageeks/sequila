package org.biodatageeks.sequila.ximmer

import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.biodatageeks.sequila.apps.PileupApp.createSparkSessionWithExtraStrategy
import org.biodatageeks.sequila.utils.SystemFilesUtil._

import java.nio.file.{Files, Paths}

class TargetCounts {

  def calculateTargetCounts(targetsPath: String, bamFiles: List[String], saveBamInfo: Boolean,
                            outputPath: String): Unit = {
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

      if (saveBamInfo) {
        val countQuery = "Select count(*) from reads"
        val bam_info_output = outputPath + "/bam_info/" + sample.toString
        Files.createDirectories(Paths.get(bam_info_output))

        ss.sql(countQuery)
          .coalesce(1)
          .write
          .mode("overwrite")
          .csv(bam_info_output)
      }

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

      val target_counts_output = outputPath + "/target_counts_output/" + sample
      Files.createDirectories(Paths.get(target_counts_output))

      ss.sql(intervalJoinQuery)
        .createOrReplaceTempView("result");

      //Uzupelnienie przedziałów z zerowym pokryciem
      //Obejscie problemu - strategia intevalJoin lapie tylko inner joina, zamiast zrobic left joina
      //Robimy jest drugi sql na pliku BED i wyniku poprzedniego sqla (podobny rozmiar co bed)
      val includeAllTargetsQuery =
      """SELECT *
        |FROM (
        |   SELECT t._c0 AS Chr,
        |          t._c1 AS Start,
        |          t._c2 AS End,
        |          0 AS codex_cov,
        |          0 AS cnmops_cov,
        |          0 AS ed_cov,
        |          0 AS conifer_cov
        |   FROM targets t
        |   WHERE NOT EXISTS (
        |         SELECT 1
        |         FROM result r
        |         WHERE r.chr = t._c0 AND r.start = t._c1 AND r.end = t._c2)
        |   UNION
        |   SELECT * FROM result
        |   )
        |ORDER BY chr, CAST(start AS INTEGER)
        |""".stripMargin

      ss.sql(includeAllTargetsQuery)
        .coalesce(1)
        .write
        .mode("overwrite")
        .csv(target_counts_output)
    }

    ss.stop()
  }

}
