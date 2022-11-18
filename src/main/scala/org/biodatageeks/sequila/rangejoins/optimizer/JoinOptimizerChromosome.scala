package org.biodatageeks.sequila.rangejoins.optimizer

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.biodatageeks.sequila.rangejoins.optimizer.RangeJoinMethod.RangeJoinMethod
import org.biodatageeks.sequila.utils.InternalParams
import org.openjdk.jol.info.GraphLayout


class JoinOptimizerChromosome [T <: Object] (spark: SparkSession, rdd: RDD[T], rddCount : Long) {

  val logger: Logger =  Logger.getLogger(this.getClass.getCanonicalName)
  val maxBroadcastSize: Double = spark.sqlContext
    .getConf(InternalParams.maxBroadCastSize,"0") match {
    case "0" => 0.1*scala.math.max((spark.sparkContext.getConf.getSizeAsBytes("spark.driver.memory","0")),1024*(1024*1024)) //defaults 128MB or 0.1 * Spark Driver's memory
    case _ => spark.sqlContext.getConf(InternalParams.maxBroadCastSize).toLong }
  val estBroadcastSize: Long = estimateBroadcastSize(rdd,rddCount)

   private def estimateBroadcastSize(rdd: RDD[T], rddCount: Long): Long = {
     try{
       (GraphLayout.parseInstance(rdd.first()).totalSize() * rddCount)
     }
     catch {
       case e @ (_ : NoClassDefFoundError | _ : ExceptionInInitializerError ) => {
         logger.info("Method ObjectSizeCalculator.getObjectSize not available falling back to Spark methods")
         SizeEstimator.estimate(rdd.first()) * rddCount
       }
     }
     //FIXME: Do not know why the size ~10x the actual size is- Spark row representation or getObject size in bits???
  }

  def debugInfo: String = {
    s"""
       |Estimated broadcast structure size is ~ ${math.rint(100*estBroadcastSize/1024.0)/100} kb
       |${InternalParams.maxBroadCastSize} is set to ${(maxBroadcastSize/1024).toInt} kb"
       |Using ${getRangeJoinMethod.toString} join method
     """.stripMargin
  }

  /**
    * Choose range join method to use basic on estimated size of the underlying data struct for broadcast
    * @param rdd
    * @return
    */
  def getRangeJoinMethod : RangeJoinMethod = {
    if (estBroadcastSize <= maxBroadcastSize)
      RangeJoinMethod.JoinWithRowBroadcast
    else
      RangeJoinMethod.TwoPhaseJoin
  }

}
