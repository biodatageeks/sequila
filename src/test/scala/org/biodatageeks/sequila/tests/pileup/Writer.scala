package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.biodatageeks.sequila.utils.Columns

object Writer {

  val mapToString = (map: Map[Byte, Short]) => {
    if (map == null)
      "null"
    else
      map.map({
        case (k, v) => k.toChar -> v
      }).toSeq.sortBy(_._1).mkString.replace(" -> ", ":")
  }



  def saveToFile(spark: SparkSession, res: Dataset[Row], path: String) = {
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

  def saveToFileWithQuals(spark: SparkSession, res: Dataset[Row], path: String) = {
    val outputColumns =  res.columns

    res
      .selectExpr(outputColumns: _*)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(path)
  }
}
