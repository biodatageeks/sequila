//package org.biodatageeks.sequila.apps
//
//import org.apache.spark.sql.SequilaSession
//import org.biodatageeks.sequila.ximmer.converters.GngsConverter
//import org.rogach.scallop.ScallopConf
//
//import java.io.File
//import java.nio.file.{Files, Paths}
//
//object XhmmCoverage extends SequilaApp {
//
//  class RunConf(args: Array[String]) extends ScallopConf(args) {
//
//    val per_base_dir = opt[String](required = true)
//    val targets = opt[String](required = true)
//    val output_path = opt[String](required = true)
//    verify()
//  }
//
//  def main(args: Array[String]): Unit = {
//
//    val runConf = new RunConf(args)
//    val spark = createSparkSessionWithJoinOrder()
//    val ss = SequilaSession(spark)
//
//    val targets = spark
//      .read
//      .option("header", "false")
//      .option("delimiter", "\t")
//      .csv(runConf.targets())
//    targets.createOrReplaceTempView("targets")
//
//    val sampleDirs = findAllSamples(runConf.per_base_dir())
//
//    for (sampleDir <- sampleDirs) {
//      val perBaseCoverageFile = findCsvFile(sampleDir)
//      val perBaseCoverage = spark
//        .read
//        .option("header", "false")
//        .option("delimiter","|") //TODO kkobylin do zmiany jesli zmieni sie sposob liczenia perBase
//        .csv(perBaseCoverageFile)
//      perBaseCoverage.createOrReplaceTempView("reads_pb_cov")
//
//      //Potrzebne przyporządkowanie pokryć per base do przedziałów
//      val joinQuery =
//        """SELECT t._c0 AS Chr,
//          |t._c1 AS Start,
//          |t._c2 AS End,
//          |r._c4 AS per_base_cov
//            FROM reads_pb_cov r INNER JOIN targets t
//          |ON (
//          |  t._c0 = concat('chr', r._c0)
//          |  AND
//          |  r._c1 >= CAST(t._c1 AS INTEGER)
//          |  AND
//          |  r._c1 <= CAST(t._c2 AS INTEGER)
//          |)
//          |ORDER BY t._c0, CAST(t._c1 AS INTEGER) desc
//          |""".stripMargin
//
//      val sample = getDirName(sampleDir)
//      val xhmm_temp_output = runConf.output_path() + "/xhmm_temp/" + sample
//      Files.createDirectories(Paths.get(xhmm_temp_output))
//
//      ss.sql(joinQuery)
//        .coalesce(1)
//        .write
//        .mode("overwrite")
//        .csv(xhmm_temp_output)
//
//      new GngsConverter().calculateStatsAndConvertToGngsFormat(xhmm_temp_output, runConf.targets(), runConf.output_path(), sample)
//    }
//  }
//
//  private def findAllSamples(dirPath: String) : List[String] = {
//    val dir = new File(dirPath)
//    if (!dir.exists() || !dir.isDirectory) {
//      throw new IllegalArgumentException("Directory path should be provided")
//    }
//
//    dir.listFiles()
//      .filter(_.isDirectory)
//      .map(_.getPath)
//      .map(x => x.replace("\\", "/"))
//      .toList
//  }
//
//  private def findCsvFile(dirPath: String) : String = {
//    val dir = new File(dirPath)
//    if (!dir.exists() || !dir.isDirectory) {
//      throw new IllegalArgumentException("Directory path should be provided")
//    }
//
//    dir.listFiles
//      .filter(_.isFile)
//      .filter(_.getName.endsWith(".csv"))
//      .map(_.getPath)
//      .map(x => x.replace("\\", "/"))
//      .toList
//      .head
//  }
//
//  private def getDirName(dirPath: String) : String = {
//    val dir = new File(dirPath)
//    if (!dir.exists() || !dir.isDirectory) {
//      throw new IllegalArgumentException("Directory path should be provided")
//    }
//
//    dir.getName
//  }
//}
