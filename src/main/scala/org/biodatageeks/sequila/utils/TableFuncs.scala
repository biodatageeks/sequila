package org.biodatageeks.sequila.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object TableFuncs{

  def getTableMetadata(spark:SparkSession, tableName:String) = {
    val catalog = spark.sessionState.catalog
    val tId = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    catalog.getTableMetadata(tId)
  }

  def getTableDirectory(spark: SparkSession, tableName:String) ={
    getTableMetadata(spark,tableName)
      .location
      .toString
      .split('/')
      .dropRight(1)
      .mkString("/")
  }

  def getExactSamplePath(spark: SparkSession, path:String) = {
    val fs = if(path.toLowerCase.startsWith("hdfs"))
      FileSystem.get(spark.sparkContext.hadoopConfiguration)
    else {
      new Path(path)
        .getFileSystem(spark.sparkContext.hadoopConfiguration)
    }
    val statuses = fs.globStatus(new org.apache.hadoop.fs.Path(path))
    statuses.head.getPath.toString
  }

  def getParentFolderPath(spark: SparkSession, path: String): String = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    (new org.apache.hadoop.fs.Path(path)).getParent.toString
  }

  def getAllSamples(spark: SparkSession, path:String) = {
    val fs = if(path.toLowerCase.startsWith("hdfs"))
      FileSystem.get(spark.sparkContext.hadoopConfiguration)
    else {
      new Path(path)
        .getFileSystem(spark.sparkContext.hadoopConfiguration)
    }
    val statuses = fs.globStatus(new org.apache.hadoop.fs.Path(path))
    statuses
      .map(_.getPath.toString.split('/').takeRight(1).head.split('.').take(1).head)
  }
}