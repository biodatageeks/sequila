//package org.biodatageeks.sequila.apps
//
//import org.biodatageeks.sequila.ximmer.converters.ExomeDepthConverter
//import org.rogach.scallop.ScallopConf
//
//import java.io.File
//import java.nio.file.{Files, Paths}
//
//object ExomeDepthCoverage extends SequilaApp {
//
//  class RunConf(args: Array[String]) extends ScallopConf(args) {
//    val target_count_dir = opt[String](required = true)
//    val output_path = opt[String](required = true)
//    verify()
//  }
//
//  def main(args: Array[String]): Unit = {
//    val runConf = new RunConf(args)
//    val targetCountSamples = findSampleDirs(runConf.target_count_dir())
//    val outputDirPath = runConf.output_path() + "/exome_depth_output"
//    Files.createDirectories(Paths.get(outputDirPath))
//    val sampleNames = targetCountSamples.map(x => findSampleName(x))
//    val targetCountFiles = targetCountSamples.map(x => findCsvFile(x))
//
//    new ExomeDepthConverter().convertToExomeDepthFormat(targetCountFiles, sampleNames, outputDirPath)
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
//  private def findSampleName(dirPath: String) : String = {
//    val dir = new File(dirPath)
//    if (!dir.exists() || !dir.isDirectory) {
//      throw new IllegalArgumentException("Directory path should be provided")
//    }
//
//    dir.getName
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
//}
