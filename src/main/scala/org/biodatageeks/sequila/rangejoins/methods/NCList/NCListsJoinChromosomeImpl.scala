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

package org.biodatageeks.sequila.rangejoins.methods.NCList

import org.apache.spark.SparkContext
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.sequila.rangejoins.NCList.{Interval, NCListTree}
import org.biodatageeks.sequila.rangejoins.common.performance.timers.IntervalTreeTimer.IntervalTreeHTSLookup
import org.biodatageeks.sequila.rangejoins.common.performance.timers.NCListTimer._
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeJoinOptimChromosomeImpl.calcOverlap

import scala.collection.immutable.Stream.Empty

object NCListsJoinChromosomeImpl extends Serializable {

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
                  rdd1: RDD[((String,Interval[Int]), InternalRow)],
                  rdd2: RDD[((String,Interval[Int]), InternalRow)]): RDD[(InternalRow, InternalRow)] = {
    val indexedRdd1 = rdd1
      .instrument()
      .zipWithIndex()
      .map(_.swap)


    /* Collect only Reference regions and the index of indexedRdd1 */
    val localIntervals = indexedRdd1.map(x => (x._2._1, x._1)).collect()
    /* Create and broadcast an interval tree */
    val nclist = NCListBuild.time{sc.broadcast(new NCListTreeChromosome[Long](localIntervals))}

    val joinedRDD = rdd2
      .instrument()
      .mapPartitions(p=> {
        p.map(r=> {
          IntervalTreeHTSLookup.time {
            val ncl = nclist.value.getAllOverlappings(r._1)
            if(ncl != Nil) {
                  ncl
                    .map(k => (k,r._2))
                    .toIterator
              }
            else Iterator.empty
          }
        })
      })
      .flatMap(r=>r)

    indexedRdd1
      .map(r=>(r._1,r._2._2))
      .join(joinedRDD.map(r=>(r._1._2,r._2)))
      .map(r=>r._2)
  }


}
