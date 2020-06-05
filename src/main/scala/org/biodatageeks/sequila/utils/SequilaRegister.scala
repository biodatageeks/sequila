package org.biodatageeks.sequila.utils

import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.SequilaDataSourceStrategy
import org.biodatageeks.sequila.utvf.GenomicIntervalStrategy
import org.biodatageeks.sequila.coverage.CoverageStrategy
import org.biodatageeks.sequila.pileup.PileupStrategy
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim

object SequilaRegister {

  def register(spark : SparkSession) = {
    spark.experimental.extraStrategies =
      Seq(
        new SequilaDataSourceStrategy(spark),
        new IntervalTreeJoinStrategyOptim(spark),
        new CoverageStrategy(spark),
        new PileupStrategy(spark),
        new GenomicIntervalStrategy(spark)

      )
    /*Set params*/
    spark
      .sparkContext
      .hadoopConfiguration
      .setInt("mapred.max.split.size", spark.sqlContext.getConf(InternalParams.InputSplitSize,"134217728").toInt)

    spark
      .sqlContext
      .setConf(InternalParams.IOReadAlignmentMethod,"hadoopBAM")

    spark
      .sqlContext
      .setConf(InternalParams.BAMValidationStringency, ValidationStringency.LENIENT.toString)

    spark
      .sqlContext
      .setConf(InternalParams.EnableInstrumentation, "false")
  }

}
