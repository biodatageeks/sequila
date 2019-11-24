
import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.rangejoins.common.metrics.MetricsCollector
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

val metricsTable = "granges.metrics"

sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)
case class PosRecord(contigName:String,start:Int,end:Int)
spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil

val alignments = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat]("/data/granges/NA12878.ga2.exome.maq.recal.bam").map(_._2.get).map(r=>PosRecord(r.getContig,r.getStart,r.getEnd))

val reads=alignments.toDF
reads.createOrReplaceTempView("reads")

val targets = spark.read.parquet("/data/granges/tgp_exome_hg18.adam")
targets.createOrReplaceTempView("targets")

val query="""    SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
            |         ON (targets.contigName=reads.contigName
            |         AND
            |         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
            |         AND
            |         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
            |         )
            |         GROUP BY targets.contigName,targets.start,targets.end"""


spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (100 *1024*1024).toString)
val mc = new  MetricsCollector(spark,metricsTable)
mc.initMetricsTable
mc.runAndCollectMetrics(
  "q_featurecounts_bam_wes",
  "spark_granges_it_bc_all",
  Array("reads","targets"),
  query,
  true
)





val reads = spark.read.parquet("/data/granges/NA12878.ga2.exome.maq.recal.adam")
reads.createOrReplaceTempView("reads")

val targets = spark.read.parquet("/data/granges/tgp_exome_hg18.adam")
targets.createOrReplaceTempView("targets")
val mc = new  MetricsCollector(spark,metricsTable)
mc.initMetricsTable
mc.runAndCollectMetrics(
  "q_featurecounts_adam_wes",
  "spark_granges_it_bc_all",
  Array("reads","targets"),
  query,
  true
)