package org.biodatageeks.sequila.tests.rangejoins

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.sequila.apps.FeatureCounts.Region
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.utils.Columns
import org.scalatest.{BeforeAndAfter, FunSuite}


class MultisampleBAMTestSuite
    extends FunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext {

  val bamPath: String = getClass.getResource("/multisample").getPath + "/*.bam"
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val tableNameBAM = "reads"

  before {
    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(
      spark) :: Nil
    spark.sql(s"DROP TABLE IF EXISTS $tableNameBAM")
    spark.sql(s"""
         |CREATE TABLE $tableNameBAM
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)

  }

  test("Multisample BAM Table") {
    assert(
      spark
        .sql(s"SELECT * FROM $tableNameBAM")
        .count === 9516L)
  }

  test("Sample name") {
    assert(spark
      .sql(
        s"SELECT ${Columns.SAMPLE} FROM $tableNameBAM order by ${Columns.SAMPLE}")
      .first
      .getString(0) === "NA12877")
  }

  test("Feature counts with multiple samples") {
    val query =
      s"""SELECT ${Columns.SAMPLE}, count(*), targets.${Columns.CONTIG}, targets.${Columns.START}, targets.${Columns.END}
              FROM reads JOIN targets
        |ON (
        |  targets.${Columns.CONTIG}=reads.${Columns.CONTIG}
        |  AND
        |  reads.${Columns.END} >= targets.${Columns.START}
        |  AND
        |  reads.${Columns.START} <= targets.${Columns.END}
        |)
        |GROUP BY ${Columns.SAMPLE}, targets.${Columns.CONTIG}, targets.${Columns.START}, targets.${Columns.END}
        |HAVING ${Columns.CONTIG}='1' AND  ${Columns.START}=20138 AND  ${Columns.END}=20294 and ${Columns.SAMPLE}='NA12878'""".stripMargin
    val targets = spark.sqlContext
      .createDataFrame(Array(Region("1", 20138, 20294)))
    targets
      .createOrReplaceTempView("targets")
    assert(spark.sql(query).first().getLong(1) === 1484L)
  }

  case class GeneRegion(contig:String, pos_start:Int, pos_end:Int, gene_id:String, strand:String)


  test("Multisample groupby - cast issue") {

    val schema = new StructType()
      .add(StructField(Columns.CONTIG, StringType))
      .add(StructField(Columns.START, IntegerType))
      .add(StructField(Columns.END, IntegerType))
      .add(StructField("gene_id", StringType))
      .add(StructField(Columns.STRAND, StringType))

    val ss = SequilaSession(spark)
    val query =
      s"""
        |SELECT targets.gene_id as GeneId, targets.${Columns.CONTIG} as Chr,
        |targets.${Columns.START} as Start, targets.${Columns.END} as End, targets.${Columns.STRAND} as Strand,
        |count(*) as Counts
        |FROM targets JOIN reads on
        |(targets.${Columns.CONTIG} = reads.${Columns.CONTIG}  AND CAST(reads.${Columns.END} as Integer) >= CAST(targets.${Columns.START} as Integer)
        |AND CAST(reads.${Columns.START} as Integer) <= CAST(targets.${Columns.END} as Integer) )
        |GROUP BY targets.gene_id, targets.${Columns.CONTIG}, targets.${Columns.START}, targets.${Columns.END}, targets.${Columns.STRAND}
      """.stripMargin

    import spark.implicits._

    val targets = ss.sqlContext
      .createDataFrame(Array(GeneRegion("1", 20138, 20294, "TestGene", "+")))
    targets
      .createOrReplaceTempView("targets")

    ss.sql(query).count
    assert(spark.sql(query).first().getLong(5) === 4452L)
  }

  test("Multisample - partition pruning one sample test") {

    val query =
      s"""
        |SELECT ${Columns.SAMPLE}, ${Columns.START}, ${Columns.CIGAR}
        |FROM reads
        |WHERE ${Columns.SAMPLE}='NA12879' AND ${Columns.CONTIG}='1'
        |LIMIT 5
      """.stripMargin

    assert(spark.sql(query).first().getString(0) === "NA12879")
  }

  test("Multisample - partition pruning many samples test") {

    val query =
      s"""
        |SELECT ${Columns.SAMPLE}, count(*)
        |FROM reads
        |WHERE ${Columns.SAMPLE} IN('NA12878','NA12879')
        |GROUP BY ${Columns.SAMPLE}
        |ORDER BY ${Columns.SAMPLE}
      """.stripMargin

    val res = spark.sql(query).take(2)
    assert(res(0).getString(0) === "NA12878" && res(0).getLong(1) === 3172L)
    assert(res(1).getString(0) === "NA12879" && res(1).getLong(1) === 3172L)

  }

}
