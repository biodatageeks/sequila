//package org.biodatageeks.sequila.apps
//
//import org.biodatageeks.sequila.ximmer.converters.ConiferConverter
//import org.rogach.scallop.ScallopConf
//
//import java.io.File
//import java.nio.file.{Files, Paths}
//
//object ConiferCoverage extends SequilaApp {
//
//  class RunConf(args: Array[String]) extends ScallopConf(args) {
//    val target_count_dir = opt[String](required = true)
//    val bam_info_dir = opt[String](required = true)
//    val output_path = opt[String](required = true)
//    verify()
//  }
//
//  def main(args: Array[String]): Unit = {
//    val runConf = new RunConf(args)
//    val targetCountSamples = findSampleDirs(runConf.target_count_dir())
//    val bamInfoSamples = findSampleDirs(runConf.bam_info_dir())
//    val converter = new ConiferConverter()
//    val outputDir = runConf.output_path() + "/conifer/rpkms/"
//    Files.createDirectories(Paths.get(outputDir))
//
//    for (targetCountSample <- targetCountSamples; bamInfoSample <- bamInfoSamples) {
//      val sample = getDirName(targetCountSample)
//      val targetCountFile = findCsvFile(targetCountSample)
//      val bamInfoFile = findCsvFile(bamInfoSample)
//      converter.convertToConiferFormat(sample, targetCountFile, bamInfoFile, outputDir)
//    }
//  }
//
//  private def findSampleDirs(dirPath: String): List[String] = {
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
