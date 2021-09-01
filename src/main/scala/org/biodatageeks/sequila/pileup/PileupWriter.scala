package org.biodatageeks.sequila.pileup

import org.apache.spark.sql.{DataFrame, SaveMode}

object PileupWriter {

  def save (df: DataFrame, path: String): Unit = {
    if (!df.schema.fields.exists(p=>p.dataType.typeName.contains("map"))) {
      saveSpecificColumns(df, df.columns, path)
    } else {
      val outputColumns = castMapFieldsToString(df)
      saveSpecificColumns(df, outputColumns, path)
    }
  }

  private def castMapFieldsToString(df: DataFrame): Array[String] = {
    val outputColumns = df.columns
    df
      .schema
      .fields
      .zipWithIndex
      .foreach { case (col, ind) => if (col.dataType.typeName.contains("map")) outputColumns(ind) = s"cast(${col.name} as string)" }
    outputColumns
  }

  private def saveSpecificColumns(df: DataFrame, columns: Array[String], path: String): Unit = {
    df
      .selectExpr(columns: _*)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter","|")
      .csv(path)
  }
}
