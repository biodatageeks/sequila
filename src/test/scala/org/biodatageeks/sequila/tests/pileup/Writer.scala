package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

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
    res
      .selectExpr("contig", "pos_start", "pos_end", "ref", "cast(coverage as int)", "mapToString(alts)")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(path)
  }
}
