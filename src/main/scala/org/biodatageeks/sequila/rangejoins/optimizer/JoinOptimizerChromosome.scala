package org.biodatageeks.sequila.rangejoins.optimizer

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.util.SizeEstimator
import org.biodatageeks.sequila.rangejoins.IntervalTree.{Interval, IntervalWithRow}
import org.biodatageeks.sequila.rangejoins.optimizer.RangeJoinMethod.RangeJoinMethod
import org.biodatageeks.sequila.utils.InternalParams


class JoinOptimizerChromosome(spark: SparkSession, rdd: RDD[(String,Interval[Int],InternalRow)], rddCount : Long) {

  val logger =  Logger.getLogger(this.getClass.getCanonicalName)
  val maxBroadcastSize = spark.sqlContext
    .getConf(InternalParams.maxBroadCastSize,"0") match {
    case "0" => 0.1*scala.math.max((spark.sparkContext.getConf.getSizeAsBytes("spark.driver.memory","0")),1024*(1024*1024)) //defaults 128MB or 0.1 * Spark Driver's memory
    case _ => spark.sqlContext.getConf(InternalParams.maxBroadCastSize).toLong }
  val estBroadcastSize = estimateBroadcastSize(rdd,rddCount)


   private def estimateBroadcastSize(rdd: RDD[(String,Interval[Int],InternalRow)], rddCount: Long): Long = {
     try{
       (ObjectSizeCalculator.getObjectSize(rdd.first()) * rddCount) /10
     }
     catch {
       case e @ (_ : NoClassDefFoundError | _ : ExceptionInInitializerError ) => {
         logger.info("Method ObjectSizeCalculator.getObjectSize not available falling back to Spark methods")
         SizeEstimator.estimate(rdd.first()) * rddCount
       }
     }
     //FIXME: Do not know why the size ~10x the actual size is- Spark row representation or getObject size in bits???
  }

  def debugInfo = {
    s"""
       |Broadcast structure size is ~ ${math.rint(100*estBroadcastSize/1024.0)/100} kb
       |${InternalParams.maxBroadCastSize} is set to ${(maxBroadcastSize/1024).toInt} kb"
       |Using ${getRangeJoinMethod.toString} join method
     """.stripMargin
  }

  private def estimateRDDSizeSpark(rdd: RDD[(String,Interval[Int],InternalRow)]): Long = {
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
