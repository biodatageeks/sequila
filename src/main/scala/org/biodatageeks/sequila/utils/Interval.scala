package org.biodatageeks.sequila.utils

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

trait Interval {

  val schema = StructType(
    Seq(StructField(s"${Columns.CONTIG}",StringType ),
      StructField(s"${Columns.START}",IntegerType ),
      StructField(s"${Columns.END}", IntegerType))
  )
}
