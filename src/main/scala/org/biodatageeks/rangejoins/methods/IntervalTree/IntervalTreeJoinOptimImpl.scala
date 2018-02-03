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
import org.bdgenomics.utils.instrumentation.{MetricsListener, RecordedMetrics}
import org.apache.spark.rdd.MetricsContext._
import org.biodatageeks.rangejoins.common.performance.timers.IntervalTreeTimer._
import scala.collection.JavaConversions._
import htsjdk.samtools.util.IntervalTree

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
    .collect()
  val intervalTree = IntervalTreeHTSBuild.time {
    val tree = new IntervalTreeHTS[InternalRow]()
    localIntervals
      .foreach(r => tree.put(r.start, r.end, r.row))
    sc.broadcast(tree)
  }
    val kvrdd2 = rdd2
      .instrument()
      .mapPartitions(p=>{
        p.map(r => {
          IntervalTreeHTSLookup.time {
            val record =
              intervalTree.value.overlappers(r.start, r.end)

            record
              .toIterator
              .map(k => (k.getValue, r.row))
          }
        })
      })
      .flatMap(r=>r)
      kvrdd2
  }

}
