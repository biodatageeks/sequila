package org.biodatageeks.sequila.utils

import java.io.File

object SystemFilesUtil {

  def findBamFiles(dirPath: String) : List[String] = {
    val dir = new File(dirPath)
    if (!dir.exists() || !dir.isDirectory) {
      throw new IllegalArgumentException("Directory path should be provided")
    }

    dir.listFiles
      .filter(_.isFile)
      .filter(_.getName.endsWith(".bam"))
      .map(_.getPath)
      .map(x => x.replace("\\", "/"))
      .toList
      .sorted
  }

  def getFilename(filePath: String) : String = {
    val file = new File(filePath)
    if (!file.exists() || file.isDirectory) {
      throw new IllegalArgumentException("Filepath should be provided")
    }

    file.getName.split('.')(0)
  }

  def findCsvFile(dirPath: String) : String = {
    val dir = new File(dirPath)
    if (!dir.exists() || !dir.isDirectory) {
      return null
    }

    dir.listFiles
      .filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.getPath)
      .map(x => x.replace("\\", "/"))
      .toList
      .head
  }

}
