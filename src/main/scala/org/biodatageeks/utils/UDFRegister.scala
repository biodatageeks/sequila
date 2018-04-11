package org.biodatageeks.utils

import org.apache.spark.sql.SparkSession
import org.biodatageeks.rangejoins.methods.transformations.RangeMethods

object UDFRegister {

  def register(spark: SparkSession) ={
    spark.sqlContext.udf.register("shift", RangeMethods.shift _)
    spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    spark.sqlContext.udf.register("overlap", RangeMethods.calcOverlap _)
    spark.sqlContext.udf.register("flank", RangeMethods.flank _)
    spark.sqlContext.udf.register("promoters", RangeMethods.promoters _)
    spark.sqlContext.udf.register("reflect", RangeMethods.reflect _)
  }
}
