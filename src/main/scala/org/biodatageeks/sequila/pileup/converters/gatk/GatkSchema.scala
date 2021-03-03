package org.biodatageeks.sequila.pileup.converters.gatk

import org.apache.spark.sql.types._
import org.biodatageeks.sequila.utils.Columns

object GatkSchema {
  val contig = 0
  val position = 1
  val ref = 2
  val cov = 3
  val pileupString = 4
  val altsMap = 5

  val pileupStringCol = "pileup"

  val schema: StructType = StructType(
    List(
      StructField(Columns.CONTIG, StringType, nullable = true),
      StructField(Columns.POS, IntegerType, nullable = true),
      StructField(Columns.REF, StringType, nullable = true),
      StructField(Columns.COVERAGE, ShortType, nullable = true),
      StructField(pileupStringCol, StringType, nullable = true),
    )
  )
}
