package org.biodatageeks.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.SequilaDataSourceStrategy
import org.biodatageeks.catalyst.utvf.GenomicIntervalStrategy
import org.biodatageeks.preprocessing.coverage.CoverageStrategy
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim

object SequilaRegister {

  def register(spark : SparkSession) = {
    spark.experimental.extraStrategies =
      Seq(
        new SequilaDataSourceStrategy(spark),
        new IntervalTreeJoinStrategyOptim(spark),
        new CoverageStrategy(spark),
        new GenomicIntervalStrategy(spark)
       // new BAMCTASStrategy(spark)
      )
  }

}
