

import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.rangejoins.common.metrics.MetricsCollector
import org.biodatageeks.sequila.rangejoins.methods.transformations.RangeMethods
import org.biodatageeks.sequila.utils.InternalParams

val reads = spark.read.parquet("/data/granges/NA12878.hiseq.wgs.bwa.recal.adam")
reads.createOrReplaceTempView("reads")
val targets = spark.read.parquet("/data/granges/genes_hg18_trim_reduced.adam")
targets.createOrReplaceTempView("targets")
val query = (
  s"""
     |SELECT * FROM reads JOIN targets
     |ON (targets.contigName=reads.contigName
     |AND
     |CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
     |AND
     |CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
     |)
     |
       """.stripMargin)


spark.sqlContext.udf.register("overlaplength", RangeMethods.calcOverlap _)
spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
val query2 =
  s"""
     |SELECT * FROM reads JOIN targets
     |ON (targets.contigName=reads.contigName
     |AND
     |CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
     |AND
     |CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
     | AND
     |overlaplength(reads.start,reads.end,targets.start,targets.end)>=10
     |)
     |
       """.stripMargin

val metricsTable = "granges.metrics"


val minOverlap = Array(20,99)
minOverlap.foreach(mo=> {
  spark.sqlContext.setConf(InternalParams.minOverlap,s"${mo.toString}")
  /*bdg-spark-granges - broadcast all*/
  spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
  spark.sqlContext.setConf(InternalParams.maxBroadCastSize, (100 * 1024 * 1024).toString)
  val mc1 = new MetricsCollector(spark, metricsTable)
  mc1.initMetricsTable
  mc1.runAndCollectMetrics(
    s"q_overlap_reads_target_adam_wgs_min${mo}_fix",
    "spark_granges_it_bc_all",
    Array("reads", "targets"),
    query
  )


  /*bdg-spark-granges - broadcast intervals*/
  spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
  spark.sqlContext.setConf(InternalParams.maxBroadCastSize, (1024).toString)
  val mc2 = new MetricsCollector(spark, metricsTable)
  mc2.runAndCollectMetrics(
    s"q_overlap_reads_target_adam_wgs_min${mo}_fix",
    "spark_granges_it_bc_int",
    Array("reads", "targets"),
    query
  )



  /*spark */
  spark.experimental.extraStrategies = Nil
  val mc3 = new MetricsCollector(spark, metricsTable)
  mc3.runAndCollectMetrics(
    s"q_overlap_reads_target_adam_wgs_min${mo}_fix",
    "spark_default",
    Array("reads", "targets"),
    query2
  )


}
)

System.exit(0)
