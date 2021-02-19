package org.biodatageeks.sequila.pileup.converters.gatk

import org.apache.spark.sql.types._

object GatkSchema {
  val contig = 0
  val position = 1
  val ref = 2
  val cov = 3
  val pileupString = 4
  val altsMap = 5

  val schema: StructType = StructType(
    List(
      StructField("contig", StringType, nullable = true),
      StructField("position", IntegerType, nullable = true),
      StructField("reference", StringType, nullable = true),
      StructField("coverage", ShortType, nullable = true),
      StructField("pileup", StringType, nullable = true),
    )
  )
}
