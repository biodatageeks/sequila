/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
  *
  * Code inspired by https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4741060/
  *
  *
  */

package org.biodatageeks.rangejoins.methods.IntervalTree


import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.catalyst.InternalRow

import org.biodatageeks.rangejoins.IntervalTree.{Interval}
import org.biodatageeks.rangejoins.common.performance.timers.IntervalTreeTimer._
import org.biodatageeks.rangejoins.optimizer.{JoinOptimizerChromosome, RangeJoinMethod}

import scala.collection.JavaConversions._




case class InternalRowPacker (ir: InternalRow)
object IntervalTreeJoinOptimChromosomeImpl extends Serializable {

  /**
    *
    * @param sc
    * @param rdd1
    * @param rdd2
    * @param rdd1Count
    * @param minOverlap
    * @param maxGap
    * @param maxBroadCastSize
    * @return
    */

  def overlapJoin(sc: SparkContext,
                  rdd1: RDD[(String,Interval[Int],InternalRow)],
                  rdd2: RDD[(String,Interval[Int],InternalRow)], rdd1Count:Long,
                  minOverlap:Int, maxGap: Int, maxBroadCastSize: Int): RDD[(InternalRow, InternalRow)] = {

    val logger =  Logger.getLogger(this.getClass.getCanonicalName)

    /* Collect only Reference regions and the index of indexedRdd1 */


    /**
      * Broadcast join pattern - use if smaller RDD is narrow otherwise follow 2-step join operation
      * first zipWithIndex and then perform a regular join as  suggested in
      * https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4741060/
      */

    val optimizer = new JoinOptimizerChromosome(sc,rdd1, rdd1Count, maxBroadCastSize)
    sc.setLogLevel("WARN")
    logger.warn(optimizer.debugInfo )

    if (optimizer.getRangeJoinMethod == RangeJoinMethod.JoinWithRowBroadcast) {

      val localIntervals = {
        if (maxGap != 0)
          rdd1
            .instrument()
            .map(r => (r._1, Interval(r._2.start - maxGap, r._2.end + maxGap), r._3.copy()))
        else
          rdd1
            .instrument()
            .map(r => (r._1, Interval(r._2.start, r._2.end), r._3.copy()))
      }
        .collect()

  val intervalTree = IntervalTreeHTSBuild.time {
    val tree = new IntervalTreeHTSChromosome[InternalRow](localIntervals)
    sc.broadcast(tree)
  }
    val joinedRDD = rdd2
      .instrument()
        .mapPartitions(p=> {
          p.map(r=> {
            IntervalTreeHTSLookup.time {
                intervalTree.value.getIntervalTreeByChromosome(r._1) match {
                  case Some(t) => {
                    val record = t.overlappers(r._2.start, r._2.end)
                    if(minOverlap != 1)
                      record
                        .filter(f=>calcOverlap(r._2.start,r._2.end,f.getStart,f.getEnd) >= minOverlap)
                        .flatMap(k => (k.getValue.map(s=>(s,r._3))) )
                    else
                      record
                        .flatMap(k => (k.getValue.map(s=>(s,r._3))) )
                  }
                  case _ => Iterator.empty
                }
            }
          })
        })
      .flatMap(r => r)
      joinedRDD
    }

    else {

      val intervalsWithId =
        rdd1
        .instrument()
        .zipWithIndex()

      val localIntervals =
        intervalsWithId
            .map(r=>(r._1._1,r._1._2,r._2) )
        .collect()

      /* Create and broadcast an interval tree */
      val intervalTree = IntervalTreeHTSBuild.time {
        val tree = new IntervalTreeHTSChromosome[Long](localIntervals)
        sc.broadcast(tree)
      }
      val kvrdd2 = rdd2
        .instrument()
        .mapPartitions(p => {
          p.map(r => {
            IntervalTreeHTSLookup.time {
                intervalTree.value.getIntervalTreeByChromosome(r._1) match {
                  case Some(t) =>{
                    val record = t.overlappers(r._2.start, r._2.end)
                    if(minOverlap != 1)
                      record
                      .filter(f=>calcOverlap(r._2.start,r._2.end,f.getStart,f.getEnd) >= minOverlap)
                      .flatMap(k => (k.getValue.map(s=>(s,r._3 ) ) ) )
                    else
                      record
                        .flatMap(k => (k.getValue.map(s=>(s,r._3 ))) )
                  }
                  case _ => Iterator.empty
                }
            }
          })
        })
        .flatMap(s => s)

      intervalsWithId
        .map(_.swap)
        .join(kvrdd2)
        .map(l => (l._2._2,l._2._1._3))
    }

  }

  private def calcOverlap(start1:Int, end1: Int, start2:Int, end2: Int ) = (math.min(end1,end2) - math.max(start1,start2) + 1)

}
