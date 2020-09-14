package org.biodatageeks.sequila.utils

import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.rangejoins.methods.transformations.RangeMethods
import org.biodatageeks.sequila.pileup.udfs.{AltFunctions, CoverageFunctions, QualityFunctions}

object UDFRegister {

  def register(spark: SparkSession) ={

    /*deprecated - function names should start with a bdg_ prefix */
    spark.sqlContext.udf.register("shift", RangeMethods.shift _)
    spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    spark.sqlContext.udf.register("overlap", RangeMethods.calcOverlap _)
    spark.sqlContext.udf.register("flank", RangeMethods.flank _)
    spark.sqlContext.udf.register("promoters", RangeMethods.promoters _)
    spark.sqlContext.udf.register("reflect", RangeMethods.reflect _)
    spark.sqlContext.udf.register("overlaplength",RangeMethods.calcOverlap _)
    /*end deprecated  */

    spark.sqlContext.udf.register("bdg_shift", RangeMethods.shift _)
    spark.sqlContext.udf.register("bdg_resize", RangeMethods.resize _)
    spark.sqlContext.udf.register("bdg_overlap", RangeMethods.calcOverlap _)
    spark.sqlContext.udf.register("bdg_flank", RangeMethods.flank _)
    spark.sqlContext.udf.register("bdg_promoters", RangeMethods.promoters _)
    spark.sqlContext.udf.register("bdg_reflect", RangeMethods.reflect _)
    spark.sqlContext.udf.register("bdg_overlaplength",RangeMethods.calcOverlap _)

    spark.sqlContext.udf.register("quals_to_map", QualityFunctions.qualsToMap _)
    spark.sqlContext.udf.register("quals_to_cov", QualityFunctions.qualsToCoverage _)
    spark.sqlContext.udf.register("to_charmap", QualityFunctions.qualsToCharMap _)

    spark.sqlContext.udf.register("cov_equals", CoverageFunctions.isCovEqual _)


    spark.sqlContext.udf.register("to_char", AltFunctions.byteToString _)



  }
}
