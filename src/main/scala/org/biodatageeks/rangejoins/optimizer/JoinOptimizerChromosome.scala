package org.biodatageeks.rangejoins.optimizer

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.SizeEstimator
import org.biodatageeks.rangejoins.IntervalTree.{Interval, IntervalWithRow}
import org.biodatageeks.rangejoins.optimizer.RangeJoinMethod.RangeJoinMethod


class JoinOptimizerChromosome(sc: SparkContext, rdd: RDD[(String,Interval[Int],InternalRow)], rddCount : Long) {

  val maxBroadcastSize = sc
    .getConf
    .getOption("spark.biodatageeks.rangejoin.maxBroadcastSize") match {
      case Some(size) => size.toLong
      case _ =>  10*(1024*1024) //defaults to 10Mb
    }
   val estBroadcastSize = estimateBroadcastSize(rdd,rddCount)


   private def estimateBroadcastSize(rdd: RDD[(String,Interval[Int],InternalRow)], rddCount: Long): Long = {
     (ObjectSizeCalculator.getObjectSize(rdd.first()) * rddCount) /10
     //FIXME: Do not know why the size ~10x the actual size is- Spark row representation or getObject size in bits???
  }

  def debugInfo = {
    s"""
       |Broadcast structure size is ~ ${estBroadcastSize/1024} kb
       |spark.biodatageeks.rangejoin.maxBroadcastSize is set to ${maxBroadcastSize/1024} kb"
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
      RangeJoinMethod.JointWithRowBroadcast
    else
      RangeJoinMethod.TwoPhaseJoin

  }



}
