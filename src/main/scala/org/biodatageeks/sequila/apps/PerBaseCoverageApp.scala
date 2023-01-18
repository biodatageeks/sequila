//package org.biodatageeks.sequila.apps
//
//import org.apache.spark.sql.SequilaSession
//import org.biodatageeks.sequila.pileup.PileupWriter
//import org.biodatageeks.sequila.pileup.converters.sequila.SequilaConverter
//import org.biodatageeks.sequila.utils.{Columns, InternalParams}
//import org.rogach.scallop.ScallopConf
//
//import java.io.File
//import java.nio.file.{Files, Paths}
//
//object PerBaseCoverageApp extends SequilaApp {
//
//  class RunConf(args: Array[String]) extends ScallopConf(args) {
//
//    val bam_dir = opt[String](required = true)
//    val targets = opt[String](required = true)
//    val output_path = opt[String](required = true)
//    verify()
//  }
//
//  def main(args: Array[String]): Unit = {
//
//    val runConf = new RunConf(args)
//    val spark = createSparkSessionWithExtraStrategy()
//    val ss = SequilaSession(spark)
//    //Include also positions with zero coverage
//    ss.sqlContext.setConf(InternalParams.ShowAllPositions,"true")
//
//    //ss.sqlContext.setConf(InternalParams.filterReadsByFlag, "1790")
////    ss.sqlContext.setConf(InternalParams.filterReadsByMQ, "70")
//
//    val bamFiles = findBamFiles(runConf.bam_dir.apply())
//
//
//    for (bam <- bamFiles) {
//      ss.sql(s"""DROP TABLE IF EXISTS reads""")
//      ss.sql(s"""DROP TABLE IF EXISTS reads_tmp""")
//      ss.sql(
//        s"""CREATE TABLE reads
//           |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
//           |OPTIONS(path '$bam')""".stripMargin)
//
//      var sample = ss.sql(s"SELECT DISTINCT (${Columns.SAMPLE}) from reads").first().get(0)
//
////      ss.sql(
////        """
////          |CREATE TABLE reads
////          |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
////          |AS
////          | SELECT *
////          | FROM reads_tmp r
////          | WHERE r.flag & 2308 == 0 AND r.mapq > 20
////          |""".stripMargin)
//
////      ss.sql(s"""
////                |Select *
////                |FROM reads_tmp r
////                |WHERE r.flag & 2308 == 0 AND r.mapq > 20
////                |""".stripMargin)
////        .createOrReplaceTempView("reads")
//
//
//      //sample = ss.sql(s"SELECT DISTINCT (${Columns.SAMPLE}) from reads").first().get(0)
//
////      ss.sql(
////        s"""
////           |Select *
////           |from reads r
////           |where r.pos_start < 2782018
////           |""".stripMargin)
////        .createOrReplaceTempView("reads")
//
//      val coverageQuery =
//        s"""
//           |SELECT *
//           |FROM  coverage('reads', '${sample}', 'bases')
//       """.stripMargin
//
//      val block_coverage_output = runConf.output_path() + "/block_coverage_output/" + sample.toString
//      Files.createDirectories(Paths.get(block_coverage_output))
//
//      val df = ss.sql(coverageQuery)
//
////        .coalesce(1)
////        .write
////        .mode("overwrite")
////        .option("header", "false")
////        .option("delimiter", "\t")
////        .csv(block_coverage_output)
//
//      new SequilaConverter(ss).toCommonFormat(df, true)
//
//      val perBaseCoverageDataFrame = new SequilaConverter(ss).transform(block_coverage_output)
//      val per_base_coverage_output = runConf.output_path() + "/per_base_coverage_output/" + sample.toString
//      Files.createDirectories(Paths.get(per_base_coverage_output))
//      PileupWriter.save(perBaseCoverageDataFrame,per_base_coverage_output)
//    }
//
//  }
//
//  private def findBamFiles(dirPath: String) : List[String] = {
//    val dir = new File(dirPath)
//    if (!dir.exists() || !dir.isDirectory) {
//      throw new IllegalArgumentException("Directory path should be provided")
//    }
//
//    dir.listFiles
//      .filter(_.isFile)
//      .filter(_.getName.endsWith(".bam"))
//      .map(_.getPath)
//      .map(x => x.replace("\\", "/"))
//      .toList
//  }
//}
