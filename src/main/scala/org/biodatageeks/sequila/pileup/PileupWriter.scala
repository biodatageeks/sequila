package org.biodatageeks.sequila.pileup

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.biodatageeks.sequila.utils.Columns

object PileupWriter {

  val mapToString = (map: Map[Byte, Short]) => {
    if (map == null)
      "null"
    else
      map.map({
        case (k, v) => k.toChar -> v
      }).toSeq.sortBy(_._1).mkString.replace(" -> ", ":")
  }

  def save (df: DataFrame, path: String): Unit = {
    df
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(path)
  }

  def saveToCsvFile(spark: SparkSession, res: Dataset[Row], path: String): Unit = {
    spark.udf.register("mapToString", mapToString)
    val ind = res.columns.indexOf({Columns.ALTS})
    val outputColumns =  res.columns
    outputColumns(ind) = s"mapToString(${Columns.ALTS})"

    res
      .selectExpr(outputColumns: _*)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(path)
  }

  def saveToCsvFileWithQuals(spark: SparkSession, res: Dataset[Row], path: String): Unit = {
    val outputColumns =  res.columns

    res
      .selectExpr(outputColumns: _*)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(path)
  }

}
