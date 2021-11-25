import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.{InternalParams, SequilaSession, UDFRegister}

/*set params*/

val ss = SequilaSession(spark)

ss.sqlContext.setConf(InternalParams.useJoinOrder,"false")
ss.sqlContext.setConf(InternalParams.maxBroadCastSize, (128*1024*1024).toString)

ss.sqlContext.setConf(InternalParams.minOverlap,"1")
ss.sqlContext.setConf(InternalParams.maxGap,"0")

/*pp disabled by default*/
ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","false")

/*GKL Inflate disabled by default - better for large seq scans than index lookups*/
ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","false")

/*spark-bam disabled by default*/
ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","false")

/*register UDFs*/

UDFRegister.register(ss)

/*inject bdg-granges strategy*/
SequilaSession.register(ss)

