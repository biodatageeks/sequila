package org.biodatageeks.sequila.pileup.converters.common

import org.apache.spark.sql.types._
import org.biodatageeks.sequila.utils.Columns

object CommonPileupFormat {

  val schemaAltsQualsMap = StructType(Seq(
    StructField(Columns.CONTIG,StringType,nullable = true),
    StructField(Columns.START,IntegerType,nullable = true),
    StructField(Columns.END,IntegerType,nullable = true),
    StructField(Columns.REF,StringType,nullable = true),
    StructField(Columns.COVERAGE,ShortType,nullable = true),
    StructField(Columns.ALTS,MapType(ByteType,ShortType),nullable = true),
    StructField(Columns.QUALS,MapType(ByteType, MapType(StringType, ShortType)),nullable = true)
  ))

  val schemaAltsQualsString = StructType(Seq(
    StructField(Columns.CONTIG,StringType,nullable = true),
    StructField(Columns.START,IntegerType,nullable = true),
    StructField(Columns.END,IntegerType,nullable = true),
    StructField(Columns.REF,StringType,nullable = true),
    StructField(Columns.COVERAGE,ShortType,nullable = true),
    StructField(Columns.ALTS,StringType,nullable = true),
    StructField(Columns.QUALS,StringType,nullable = true)
  ))
}
