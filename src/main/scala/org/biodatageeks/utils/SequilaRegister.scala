package org.biodatageeks.utils

import org.apache.spark.sql.SparkSession
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim

object SequilaRegister {

  def register(spark : SparkSession) = spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
}
