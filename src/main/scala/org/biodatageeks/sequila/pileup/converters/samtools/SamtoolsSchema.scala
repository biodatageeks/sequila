package org.biodatageeks.sequila.pileup.converters.samtools

import org.apache.spark.sql.types._

object SamtoolsSchema {
    val contig = 0
    val position = 1
    val ref = 2
    val cov = 3
    val pileupString = 4
    val qualityString = 5
    val altsMap = 6
    val qualsMap = 7

    val schema: StructType = StructType(
        List(
            StructField("contig", StringType, nullable = true),
            StructField("position", IntegerType, nullable = true),
            StructField("reference", StringType, nullable = true),
            StructField("coverage", ShortType, nullable = true),
            StructField("pileup", StringType, nullable = true),
            StructField("quality", StringType, nullable = true)
        )
    )
}
