

import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.rangejoins.common.metrics.MetricsCollector

val reads = spark.read.parquet("/data/granges/NA12878.hiseq.wgs.bwa.recal.adam")
reads.createOrReplaceTempView("reads")
val targets = spark.read.parquet("/data/granges/genes_hg18_trim.adam")
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


val metricsTable = "granges.metrics"


/*bdg-spark-granges - broadcast all*/
spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (100 *1024*1024).toString)
val mc = new  MetricsCollector(spark,metricsTable)
mc.initMetricsTable
mc.runAndCollectMetrics(
  "q_overlap_reads_target_adam_wgs",
  "spark_granges_it_bc_all",
  Array("reads","targets"),
  query
)


/*bdg-spark-granges - broadcast intervals*/
spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (1024*1024).toString)
val mc = new  MetricsCollector(spark,metricsTable)
mc.runAndCollectMetrics(
  "q_overlap_reads_target_adam_wgs",
  "spark_granges_it_bc_int",
  Array("reads","targets"),
  query
)


/*genap*/
import org.biodatageeks.sequila.rangejoins.genApp.IntervalTreeJoinStrategy
spark.experimental.extraStrategies =  new IntervalTreeJoinStrategy(spark) :: Nil
val mc = new  MetricsCollector(spark,metricsTable)
mc.runAndCollectMetrics(
  "q_overlap_reads_target_adam_wgs",
  "spark_genap",
  Array("reads","targets"),
  query
)

/*spark */
spark.experimental.extraStrategies = Nil
val mc = new  MetricsCollector(spark,metricsTable)
mc.runAndCollectMetrics(
  "q_overlap_reads_target_adam_wgs",
  "spark_default",
  Array("reads","targets"),
  query
)



System.exit(0)