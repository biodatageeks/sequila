package org.biodatageeks.sequila.apps

import org.apache.spark.sql.{DataFrame, SequilaSession}
import org.biodatageeks.sequila.apps.PileupApp.createSparkSessionWithExtraStrategy
import org.biodatageeks.sequila.utils.SystemFilesUtil._
import org.biodatageeks.sequila.ximmer.converters._
import org.biodatageeks.sequila.ximmer.{PerBaseCoverage, TargetCounts}
import org.rogach.scallop.ScallopConf

import java.nio.file.{Files, Paths}
import scala.collection.mutable

object XimmerApp {

  class RunConf(args: Array[String]) extends ScallopConf(args) {
    val bam_dir = opt[String](required = true)
    val targets = opt[String](required = true)
    val output_path = opt[String](required = true)
    val spark_save = opt[Boolean](default = Some(false))
    val callers = trailArg[List[String]](required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val runConf = new RunConf(args)
    val spark = createSparkSessionWithExtraStrategy(runConf.spark_save())
    val ss = SequilaSession(spark)

    val bamFiles = findBamFiles(runConf.bam_dir())

    val shouldCallXhmm = runConf.callers().contains("xhmm")
    val shouldCallConifer = runConf.callers().contains("conifer")
    val shouldCallCodex = runConf.callers().contains("codex")
    val shouldCallCnmops = runConf.callers().contains("cnmops")
    val shouldCallExomedepth = runConf.callers().contains("exomedepth")

    var targetCountsResult = mutable.Map[String, (DataFrame, Long)]()

    if (shouldCalculateTargetCounts(runConf)) {
      val targetCountsStart = System.currentTimeMillis()
      targetCountsResult = new TargetCounts().calculateTargetCounts(spark, runConf.targets(), bamFiles, shouldCallConifer)
      val targetCountsEnd = System.currentTimeMillis()
      println("Whole targetCouns time: " + (targetCountsEnd - targetCountsStart) / 1000)
    }
    val codexTimeStart = System.currentTimeMillis()
    if (shouldCallCodex) {
      val codexOutput = runConf.output_path() + "/codex"
      Files.createDirectories(Paths.get(codexOutput))
      new CodexConverter().convertToCodexFormat(targetCountsResult, codexOutput)
    }
    val codexTimeEnd = System.currentTimeMillis()
    val cnmopsTimeStart = System.currentTimeMillis()
    if (shouldCallCnmops) {
      val cnmopsOutput = runConf.output_path() + "/cnmops"
      Files.createDirectories(Paths.get(cnmopsOutput))
      new CnMopsConverter().convertToCnMopsFormat(targetCountsResult, cnmopsOutput)
    }
    val cnmopsTimeEnd = System.currentTimeMillis()
    val coniferTimeStart = System.currentTimeMillis()
    if (shouldCallConifer) {
      convertToConiferFormat(targetCountsResult, runConf.output_path())
    }
    val coniferTimeEnd = System.currentTimeMillis()
    val edTimeStart = System.currentTimeMillis()
    if (shouldCallExomedepth) {
      val exomedepthOutput = runConf.output_path() + "/exomedepth"
      Files.createDirectories(Paths.get(exomedepthOutput))
      new ExomeDepthConverter().convertToExomeDepthFormat(targetCountsResult, exomedepthOutput)
    }
    val edTimeEnd = System.currentTimeMillis()

    var xhmmTimeStart = System.currentTimeMillis()
    if (shouldCallXhmm) {
      val perBaseCoverageStart = System.currentTimeMillis()
      val perBaseResults = new PerBaseCoverage().calculatePerBaseCoverage(ss, bamFiles, runConf.targets())
      val perBaseCoverageEnd = System.currentTimeMillis()
      println("PerBase time: " + (perBaseCoverageEnd - perBaseCoverageStart) / 1000)
      xhmmTimeStart = System.currentTimeMillis()
      convertToXhmmFormat(runConf.output_path(), perBaseResults)
    }
    val xhmmTimeEnd = System.currentTimeMillis()

    println("xhmmConvert time: " + (xhmmTimeEnd - xhmmTimeStart) / 1000)
    println("codexConvert time: " + (codexTimeEnd - codexTimeStart) / 1000)
    println("CnmopsConvert time: " + (cnmopsTimeEnd - cnmopsTimeStart) / 1000)
    println("ConiferConvert time: " + (coniferTimeEnd - coniferTimeStart) / 1000)
    println("ExomeDepthConvert time: " + (edTimeEnd - edTimeStart) / 1000)
    println("Whole time: " + (xhmmTimeEnd - startTime) / 1000)
  }

  private def shouldCalculateTargetCounts(runConf: RunConf): Boolean = {
    val callers = List("cnmops", "exomedepth", "conifer", "codex")
    runConf.callers().exists(callers.contains)
  }

  private def convertToXhmmFormat(outputPath: String, perBaseResult: mutable.Map[String, (DataFrame, DataFrame)]) : Unit = {
    val xhmmOutput = outputPath + "/xhmm"
    Files.createDirectories(Paths.get(xhmmOutput))
    val converter = new GngsConverter()

    perBaseResult.foreach(pair =>
      converter.calculateStatsAndConvertToGngsFormat(xhmmOutput, pair._1, pair._2._1, pair._2._2)
    )
  }

  private def convertToConiferFormat(targetCountResult: mutable.Map[String, (DataFrame, Long)], outputPath: String) : Unit = {
    val coniferOutput = outputPath + "/conifer"
    Files.createDirectories(Paths.get(coniferOutput))
    val converter = new ConiferConverter()

    for (sampleResult <- targetCountResult) {
      converter.convertToConiferFormat(sampleResult, coniferOutput)
    }
  }

}
