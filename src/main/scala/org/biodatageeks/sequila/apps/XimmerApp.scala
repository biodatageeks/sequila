package org.biodatageeks.sequila.apps

import org.biodatageeks.sequila.utils.SystemFilesUtil._
import org.biodatageeks.sequila.ximmer.converters._
import org.biodatageeks.sequila.ximmer.{PerBaseCoverage, TargetCounts}
import org.rogach.scallop.ScallopConf

import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer

object XimmerApp {

  class RunConf(args: Array[String]) extends ScallopConf(args) {
    val bam_dir = opt[String](required = true)
    val targets = opt[String](required = true)
    val output_path = opt[String](required = true)
    val callers = trailArg[List[String]](required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val runConf = new RunConf(args)
    val bamFiles = findBamFiles(runConf.bam_dir())
    val tempOutput = runConf.output_path() + "/temp"
    Files.createDirectories(Paths.get(tempOutput))

    val shouldCallXhmm = runConf.callers().contains("xhmm")
    val shouldCallConifer = runConf.callers().contains("conifer")
    val shouldCallCodex = runConf.callers().contains("codex")
    val shouldCallCnmops = runConf.callers().contains("cnmops")
    val shouldCallExomedepth = runConf.callers().contains("exomedepth")

    if (shouldCallXhmm) {
      new PerBaseCoverage().calculatePerBaseCoverage(bamFiles, runConf.targets(), tempOutput)
    }
    if (shouldCalculateTargetCounts(runConf)) {
      new TargetCounts().calculateTargetCounts(runConf.targets(), bamFiles, shouldCallConifer, tempOutput)
    }

    val sampleFiles = getSampleFiles(bamFiles, tempOutput)
    val targetCountFiles = sampleFiles.toStream
      .map(x => x.targetsCount)
      .toList
    val sampleNames = sampleFiles.toStream
      .map(x => x.sampleName)
      .toList

    if (shouldCallXhmm) {
      convertToXhmmFormat(sampleFiles, runConf.targets(), runConf.output_path())
    }
    if (shouldCallCodex) {
      val codexOutput = runConf.output_path() + "/codex"
      Files.createDirectories(Paths.get(codexOutput))
      new CodexConverter().convertToCodexFormat(targetCountFiles, codexOutput)
    }
    if (shouldCallCnmops) {
      val cnmopsOutput = runConf.output_path() + "/cnmops"
      Files.createDirectories(Paths.get(cnmopsOutput))
      new CnMopsConverter().convertToCnMopsFormat(targetCountFiles, sampleNames, cnmopsOutput)
    }
    if (shouldCallConifer) {
      convertToConiferFormat(sampleFiles, runConf.output_path())
    }
    if (shouldCallExomedepth) {
      val exomedepthOutput = runConf.output_path() + "/exomedepth"
      Files.createDirectories(Paths.get(exomedepthOutput))
      new ExomeDepthConverter().convertToExomeDepthFormat(targetCountFiles, sampleNames, exomedepthOutput)
    }
  }

  private def shouldCalculateTargetCounts(runConf: RunConf): Boolean = {
    val callers = List("cnmops", "exomedepth", "conifer", "codex")
    runConf.callers().exists(callers.contains)
  }

  private def convertToXhmmFormat(samplesFiles : List[SampleFiles], targetsPath: String, outputPath: String) : Unit = {
    val xhmmOutput = outputPath + "/xhmm"
    Files.createDirectories(Paths.get(xhmmOutput))
    val converter = new GngsConverter()

    for (sampleFiles <- samplesFiles) {
      converter.calculateStatsAndConvertToGngsFormat(sampleFiles.perBase, targetsPath, xhmmOutput, sampleFiles.sampleName)
    }
  }

  private def convertToConiferFormat(samplesFiles : List[SampleFiles], outputPath: String) : Unit = {
    val coniferOutput = outputPath + "/conifer"
    Files.createDirectories(Paths.get(coniferOutput))
    val converter = new ConiferConverter()

    for (sampleFiles <- samplesFiles) {
      converter.convertToConiferFormat(sampleFiles.sampleName, sampleFiles.targetsCount, sampleFiles.bamInfo, coniferOutput)
    }
  }

  private class SampleFiles(val sampleName: String, val targetsCount: String, val bamInfo: String, val perBase: String)

  private def getSampleFiles(bamFiles: List[String], tempOutput: String) : List[SampleFiles] = {
    val samplesFiles = ListBuffer[SampleFiles]()
    for (bam <- bamFiles) {
      val sample = getFilename(bam)

      val targetCountsDir = tempOutput + "/target_counts_output/" + sample
      val bamInfoDir = tempOutput + "/bam_info/" + sample
      val targetsPerBaseDir = tempOutput + "/targets_per_base_cov/" + sample

      val targetCountsFile = findCsvFile(targetCountsDir)
      val bamInfoFile = findCsvFile(bamInfoDir)
      val targetsPerBaseFile = findCsvFile(targetsPerBaseDir)
      val sampleFiles = new SampleFiles(sample, targetCountsFile, bamInfoFile, targetsPerBaseFile)
      samplesFiles += sampleFiles
    }

    samplesFiles.toList
  }

}
