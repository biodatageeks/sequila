import org.biodatageeks.utils.{SequilaRegister, UDFRegister}

/*set params*/

spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder","false")
spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (128*1024*1024).toString)

spark.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")

/*register UDFs*/

UDFRegister.register(spark)

/*inject bdg-granges strategy*/
SequilaRegister.register(spark)

