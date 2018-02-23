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

package org.biodatageeks.rangejoins.methods.IntervalTree

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.rangejoins.IntervalTree.{Interval, IntervalWithRow}
import org.biodatageeks.rangejoins.common.performance.timers.IntervalTreeTimer._
import org.biodatageeks.rangejoins.optimizer.{JoinOptimizer, JoinOptimizerChromosome, RangeJoinMethod}

import scala.collection.JavaConversions._

object IntervalTreeJoinOptimChromosomeImpl extends Serializable {

  /**
    * Multi-joins together two RDDs that contain objects that map to reference regions.
    * The elements from the first RDD become the key of the output RDD, and the value
    * contains all elements from the second RDD which overlap the region of the key.
    * This is a multi-join, so it preserves n-to-m relationships between regions.
    *
    * @param sc A spark context from the cluster that will perform the join
    * @param rdd1 RDD of values on which we build an interval tree. Assume |rdd1| < |rdd2|
    */
  def overlapJoin(sc: SparkContext,
                  rdd1: RDD[(String,Interval[Int],InternalRow)],
                  rdd2: RDD[(String,Interval[Int],InternalRow)], rdd1Count:Long): RDD[(InternalRow, InternalRow)] = {

    val logger =  Logger.getLogger(this.getClass.getCanonicalName)

    /* Collect only Reference regions and the index of indexedRdd1 */


    /**
      * Broadcast join pattern - use if smaller RDD is narrow otherwise follow 2-step join operation
      * first zipWithIndex and then perform a regular join
      */

    val optimizer = new JoinOptimizerChromosome(sc, rdd1, rdd1Count)
    sc.setLogLevel("WARN")
    logger.warn(optimizer.debugInfo )

    if (optimizer.getRangeJoinMethod == RangeJoinMethod.JointWithRowBroadcast) {

      val localIntervals =
        rdd1
        .instrument()
        .collect()
  val intervalTree = IntervalTreeHTSBuild.time {
    val tree = new IntervalTreeHTSChromosome[InternalRow](localIntervals.toList)
    sc.broadcast(tree)
  }
    val kvrdd2 = rdd2
      .instrument()
        .mapPartitions(p=> {
          p.map(r=> {
            IntervalTreeHTSLookup.time {
              val record =
                intervalTree.value.getIntervalTreeByChromosome(r._1)
                  .overlappers(r._2.start, r._2.end)

              record
                .toIterator
                .map(k => (k.getValue, r._3
                ))
            }
          })
        })
      .flatMap(r => r)
      kvrdd2
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
        val tree = new IntervalTreeHTSChromosome[Long](localIntervals.toList)
        sc.broadcast(tree)
      }
      val kvrdd2: RDD[(Long, Iterable[InternalRow])] = rdd2
        .instrument()
        .mapPartitions(p => {
          p.map(r => {
            IntervalTreeHTSLookup.time {
              val record =
                intervalTree.value.getIntervalTreeByChromosome(r._1)
                  .overlappers(r._2.start, r._2.end)
              record
                .toIterator
                .map(k => (k.getValue,Iterable(r._3)))
            }
          })
        })
        .flatMap(r => r)
        .reduceByKey((a,b) => a ++ b)

      intervalsWithId
        .map(_.swap)
        .join(kvrdd2)
        .flatMap(l => l._2._2.map(r => (l._2._1._3, r)))
    }

  }

}
