import org.apache.spark.sql.SequilaSession
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}

/*set params*/

val ss = SequilaSession(spark)

ss.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder","false")
ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (128*1024*1024).toString)

ss.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")

/*pp disabled by default*/
ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","false")

/*GKL Inflate disabled by default - better for large seq scans than index lookups*/
  ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","false")

/*spark-bam disabled by default*/
ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","false")

/*register UDFs*/

UDFRegister.register(ss)

/*inject bdg-granges strategy*/
SequilaRegister.register(ss)

ss.sql(
  """
    |CREATE TABLE IF NOT EXISTS reads
    |USING org.biodatageeks.datasources.BAM.BAMDataSource
    |OPTIONS(path "/data/input/bams/*.bam")
  """.stripMargin)
