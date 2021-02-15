package org.biodatageeks.sequila.pileup.converters

import org.apache.spark.sql.types.{ArrayType, ByteType, IntegerType, MapType, ShortType, StringType, StructField, StructType}
import org.biodatageeks.sequila.utils.Columns

object CommonPileupFormat {
  val schema = StructType(Seq(
    StructField(Columns.CONTIG,StringType,nullable = true),
    StructField(Columns.START,IntegerType,nullable = false),
    StructField(Columns.END,IntegerType,nullable = false),
    StructField(Columns.REF,StringType,nullable = false),
    StructField(Columns.COVERAGE,IntegerType,nullable = false),
    //StructField(Columns.COUNT_REF,IntegerType,nullable = false),
   // StructField(Columns.COUNT_NONREF,IntegerType,nullable = false),
    StructField(Columns.ALTS,MapType(ByteType,ShortType),nullable = true),
    StructField(Columns.QUALS,MapType(ByteType,ArrayType(ShortType)),nullable = true)
  ))

  val fileSchema = StructType(Seq(
    StructField(Columns.CONTIG,StringType,nullable = true),
    StructField(Columns.START,IntegerType,nullable = false),
    StructField(Columns.END,IntegerType,nullable = false),
    StructField(Columns.REF,StringType,nullable = false),
    StructField(Columns.COVERAGE,IntegerType,nullable = false),
    StructField(Columns.ALTS,StringType,nullable = true),
    StructField(Columns.QUALS,StringType,nullable = true)
  ))
}