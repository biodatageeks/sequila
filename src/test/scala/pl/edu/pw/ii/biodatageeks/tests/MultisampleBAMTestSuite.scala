package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark
import org.apache.spark.sql.SequilaSession
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.scalatest.{BeforeAndAfter, FunSuite}

class MultisampleBAMTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext {


  val bamPath = getClass.getResource("/multisample").getPath+"/*.bam"
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val tableNameBAM = "reads"

  before{
    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)

  }

  test("Multisample BAM Table"){
    assert(spark
      .sql(s"SELECT * FROM ${tableNameBAM}")
      .count === 9516L)
  }

  test("Sample name"){
    assert(spark
      .sql(s"SELECT sampleId FROM ${tableNameBAM}")
      .first.getString(0) === "NA12877")
  }

  test("Feature counts with multiple samples"){
    val query ="""SELECT sampleId,count(*),targets.contigName,targets.start,targets.end
              FROM reads JOIN targets
        |ON (
        |  targets.contigName=reads.contigName
        |  AND
        |  reads.end >= targets.start
        |  AND
        |  reads.start <= targets.end
        |)
        |GROUP BY sampleId,targets.contigName,targets.start,targets.end
        |having contigName='chr1' AND  start=20138 AND  end=20294 and sampleId='NA12878'""".stripMargin
    val targets = spark
      .sqlContext
      .createDataFrame(Array(Region("chr1", 20138, 20294)))
    targets
      .createOrReplaceTempView("targets")
    assert(spark.sql(query).first().getLong(1) === 1484L)
  }

  test("Multisample groupby - cast issue") {
    val ss = new SequilaSession(spark)
    val query =
      """
        |SELECT targets.GeneId as GeneId, targets.contigName as Chr,
        |targets.Start as Start, targets.End as End, targets.Strand as Strand,
        |count(*) as Counts FROM targets join reads on
        |(targets.contigName = reads.contigName  AND CAST(reads.end as Integer) >= CAST(targets.Start as Integer)
        |AND CAST(reads.start as Integer) <= CAST(targets.End as Integer) )
        |GROUP BY targets.GeneId, targets.contigName, targets.Start, targets.End, targets.Strand
      """.stripMargin

    val targets = ss
      .sqlContext
      .createDataFrame(Array(Gene("chr1",20138,20294,"TestGene","+")))
    targets
      .createOrReplaceTempView("targets")

    ss.sql(query).explain(true)
    ss.sql(query).count
    assert(spark.sql(query).first().getLong(5) === 4452L)
  }

  test("Multisample - partition pruning one sample test"){

    val query =
      """
        |SELECT sampleId,start,cigar FROM reads where sampleId='NA12879' AND contigName='chr1' LIMIT 5
      """.stripMargin

    println(query)
    assert(spark.sql(query).first().getString(0) === "NA12879")
  }

  test("Multisample - partition pruning many samples test"){

    val query =
      """
        |SELECT sampleId,count(*) FROM reads where sampleId IN('NA12878','NA12879')
        |GROUP BY sampleId order by sampleId
      """.stripMargin

    //spark.sql(query).explain(true)
    println(query)
    val res = spark.sql(query).take(2)
    assert(res(0).getString(0) === "NA12878" && res(0).getLong(1) === 3172L)
    assert(res(1).getString(0) === "NA12879" && res(1).getLong(1) === 3172L)

  }


}
