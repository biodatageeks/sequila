import org.apache.spark.sql.SequilaSession
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}

/*set params*/

val ss = SequilaSession(spark)

ss.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder","false")
ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (128*1024*1024).toString)

ss.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")

/*register UDFs*/

UDFRegister.register(ss)

/*inject bdg-granges strategy*/
SequilaRegister.register(ss)

