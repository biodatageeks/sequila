package org.biodatageeks.sequila.pileup

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object PileupReader {

  def load(spark: SparkSession, path: String, schema: StructType, delimiter:String =",", quote:String="\""): DataFrame = {
    spark.read
      .format("csv")
      .option("delimiter", delimiter)
      .option("quote", quote)
      .schema(schema)
      .load(path)
  }
}
