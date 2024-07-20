package org.biodatageeks.sequila.tests.rangejoins

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.biodatageeks.sequila.apps.FeatureCounts.Region
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.utils.{Columns, DataQualityFuncs}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}



class FeatureCountsTestSuite
    extends AnyFunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  before {
    System.setSecurityManager(null)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil
  }

  test("Feature counts for chr1:20138-20294") {
    val query = s"""
        | SELECT count(*),targets.${Columns.CONTIG},targets.${Columns.START},targets.${Columns.END}
        | FROM reads JOIN targets
        |ON (
        |  targets.${Columns.CONTIG}=reads.${Columns.CONTIG}
        |  AND
        |  reads.${Columns.END} >= targets.${Columns.START}
        |  AND
        |  reads.${Columns.START} <= targets.${Columns.END}
        |)
        | GROUP BY targets.${Columns.CONTIG},targets.${Columns.START},targets.${Columns.END}
        | HAVING ${Columns.CONTIG}='1' AND ${Columns.START} = 20138 AND ${Columns.END} = 20294""".stripMargin

    spark.sparkContext.hadoopConfiguration.set(
      SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY,
      ValidationStringency.SILENT.toString)

    val alignments = spark.sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](
        getClass.getResource("/NA12878.slice.bam").getPath)
      .map(_._2.get)
      .map(r => Region(DataQualityFuncs.cleanContig(r.getContig), r.getStart, r.getEnd))

    val reads = spark.sqlContext
      .createDataFrame(alignments)
      .withColumnRenamed("contigName", Columns.CONTIG)
      .withColumnRenamed("start", Columns.START)
      .withColumnRenamed("end", Columns.END)

    reads.createOrReplaceTempView("reads")

    val targets = spark.sqlContext
      .createDataFrame(Array(Region("1", 20138, 20294)))
      .withColumnRenamed("contigName", Columns.CONTIG)
      .withColumnRenamed("start", Columns.START)
      .withColumnRenamed("end", Columns.END)

    targets.createOrReplaceTempView("targets")

    spark.sql(query).explain(false)
    assert(spark.sql(query).first().getLong(0) === 1484L)

  }

}
