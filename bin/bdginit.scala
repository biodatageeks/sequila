import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.rangejoins.methods.transformations.RangeMethods

/*set params*/

spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder","false")
spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (128*1024*1024).toString)

spark.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")

/*register UDFs*/
spark.sqlContext.udf.register("shift", RangeMethods.shift _)
spark.sqlContext.udf.register("resize", RangeMethods.resize _)
spark.sqlContext.udf.register("overlap", RangeMethods.calcOverlap _)

/*inject bdg-granges strategy*/
spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil

