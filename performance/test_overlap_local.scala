

import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.rangejoins.common.metrics.MetricsCollector
import org.biodatageeks.sequila.utils.InternalParams

val ref = spark.read.parquet("src/test/resources/refFlat.adam")
ref.createOrReplaceTempView("ref")

val snp = spark.read.parquet("src/test/resources/snp150Flagged.adam")
snp.createOrReplaceTempView("snp")

val query = (
  s"""
     |SELECT * FROM snp JOIN ref
     |ON (ref.contigName=snp.contigName
     |AND
     |CAST(snp.end AS INTEGER)>=CAST(ref.start AS INTEGER)
     |AND
     |CAST(snp.start AS INTEGER)<=CAST(ref.end AS INTEGER)
     |)
     |
       """.stripMargin)



/*bdg-spark-granges - broadcast all*/
val mc = new  MetricsCollector(spark,"testMetrics")
mc.dropMetricsTable
mc.initMetricsTable
spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (100 *1024*1024).toString)
mc.runAndCollectMetrics(
  "q_overlap_snp_ref_adam",
  "spark_granges_it_bc_all",
  Array("snp","ref"),
  query
)


/*bdg-spark-granges - broadcast intervals*/
val mc = new  MetricsCollector(spark,"testMetrics")
spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
spark.sqlContext.setConf(InternalParams.maxBroadCastSize, (1024*1024).toString)
mc.runAndCollectMetrics(
  "q_overlap_snp_ref_adam",
  "spark_granges_it_bc_int",
  Array("snp","ref"),
  query
)

spark.sql(
  """
    |SELECT * FROM testMetrics
  """.stripMargin)
.show(false)

System.exit(0)