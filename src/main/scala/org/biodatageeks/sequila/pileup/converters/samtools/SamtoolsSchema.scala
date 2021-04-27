package org.biodatageeks.sequila.pileup.converters.samtools

import org.apache.spark.sql.types._
import org.biodatageeks.sequila.utils.Columns

object SamtoolsSchema {
    val contig = 0
    val position = 1
    val ref = 2
    val cov = 3
    val pileupString = 4
    val qualityString = 5
    val altsMap = 6
    val qualsMap = 7

    val pileupStringCol = "pileup"
    val qualityStringCol = "quality"


    val schema: StructType = StructType(
        List(
            StructField(Columns.CONTIG, StringType, nullable = true),
            StructField(Columns.POS, IntegerType, nullable = true),
            StructField(Columns.REF, StringType, nullable = true),
            StructField(Columns.COVERAGE, ShortType, nullable = true),
            StructField(pileupStringCol, StringType, nullable = true),
            StructField(qualityStringCol, StringType, nullable = true)
        )
    )
}
