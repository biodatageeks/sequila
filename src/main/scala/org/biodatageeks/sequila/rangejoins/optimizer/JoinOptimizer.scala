package org.biodatageeks.sequila.rangejoins.optimizer

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalWithRow
import org.biodatageeks.sequila.rangejoins.optimizer.RangeJoinMethod.RangeJoinMethod


class JoinOptimizer(sc: SparkContext, rdd: RDD[IntervalWithRow[Int]], rddCount : Long) {

  val logger =  Logger.getLogger(this.getClass.getCanonicalName)

  val maxBroadcastSize = sc
    .getConf
    .getOption("spark.biodatageeks.rangejoin.maxBroadcastSize") match {
      case Some(size) => size.toLong
      case _ => 0.1*scala.math.max((sc.getConf.getSizeAsBytes("spark.driver.memory","0")).toLong,1024*(1024*1024)) //defaults 128MB or 0.1 * Spark Driver's memory
    }
   val estBroadcastSize = estimateBroadcastSize(rdd,rddCount)


   private def estimateBroadcastSize(rdd: RDD[IntervalWithRow[Int]], rddCount: Long): Long = {
     try{
       (ObjectSizeCalculator.getObjectSize(rdd.first()) * rddCount) /10
     }
     catch {
       case e @ (_ : NoClassDefFoundError | _ : ExceptionInInitializerError )  => {
         logger.warn("Method ObjectSizeCalculator.getObjectSize not available falling back to Spark methods")
         SizeEstimator.estimate(rdd.first()) * rddCount
       }
     }
     //FIXME: Do not know why the size ~10x the actual size is- Spark row representation or getObject size in bits???
  }

  def debugInfo = {
    s"""
       |Broadcast structure size is ~ ${estBroadcastSize/1024} kb
       |spark.biodatageeks.rangejoin.maxBroadcastSize is set to ${maxBroadcastSize/1024} kb"
       |Using ${getRangeJoinMethod.toString} join method
     """.stripMargin
  }

  private def estimateRDDSizeSpark(rdd: RDD[IntervalWithRow[Int]]): Long = {
    math.round(SizeEstimator.estimate(rdd)/1024.0)
  }

  /**
    * Choose range join method to use basic on estimated size of the underlying data struct for broadcast
    * @param rdd
    * @return
    */
  def getRangeJoinMethod : RangeJoinMethod ={

    if (estimateBroadcastSize(rdd, rddCount) <= maxBroadcastSize)
      RangeJoinMethod.JoinWithRowBroadcast
    else
      RangeJoinMethod.TwoPhaseJoin

  }



}
