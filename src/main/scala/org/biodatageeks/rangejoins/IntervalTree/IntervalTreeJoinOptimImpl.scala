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

package org.biodatageeks.rangejoins.IntervalTree


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener}
import org.apache.spark.rdd.MetricsContext._


import common.performance.timers.IntervalTreeTimer._

object IntervalTreeJoinOptimImpl extends Serializable {

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
                  rdd1: RDD[(IntervalWithRow[Int])],
                  rdd2: RDD[(IntervalWithRow[Int])]): RDD[(InternalRow, InternalRow)] = {

    /* Collect only Reference regions and the index of indexedRdd1 */


  val localIntervals =
    rdd1
    .instrument()
    // .map(r=>IntervalWithRow(r._1.start,r._1.end,r._2))
    .collect()
  val intervalTree = IntervalTreeBuild.time {
    sc.broadcast(new IntervalTree[Int](localIntervals.toList))
  }

    val kvrdd2 = rdd2
        .instrument()
     // .map(x => (x._2,intervalTree.value.getAllOverlappings(IntervalWithRow(x._1.start,x._1.end,x._2))))
      .map(x =>(x.row,IntervalTreeLookup.time{ intervalTree.value.getAllOverlappings(x) } ) )
      .mapPartitions(p=>{
        p.map(record => {
          if(record._2 != Nil)
            record._2.map(overlap => (overlap.row,record._1))
          else Iterator.empty
        }
        )
      }).flatMap(r=>r)
//      .filter(x => x._2 != Nil)
//      .flatMap(r=>r._2.map(k=>(k.row,r._1))) //FIXME swap just for passing unit tests
      kvrdd2

  }

}
