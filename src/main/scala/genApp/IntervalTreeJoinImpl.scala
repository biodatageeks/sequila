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

package genApp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow


import org.apache.spark.rdd.MetricsContext._
import common.performance.timers.IntervalTreeTimer._


object IntervalTreeJoinImpl extends Serializable {

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
                  rdd1: RDD[(Interval[Int], InternalRow)],
                  rdd2: RDD[(Interval[Int], InternalRow)]): RDD[(InternalRow, Iterable[InternalRow])] = {
    val indexedRdd1 = rdd1
      .instrument()
      .zipWithIndex()
      .map(r=>(r._2.toInt,r._1))

    /* Collect only Reference regions and the index of indexedRdd1 */
    val localIntervals = indexedRdd1.map(x => (x._2._1, x._1)).collect()
    /* Create and broadcast an interval tree */
    val intervalTree = IntervalTreeBuild.time {sc.broadcast(new IntervalTree[Int](localIntervals.toList)) }
    val kvrdd2: RDD[(Int, Iterable[InternalRow])] = rdd2
        .instrument()
      // join entry with the intervals returned from the interval tree
      .map(x => (IntervalTreeLookup.time{intervalTree.value.getAllOverlappings(x._1)}, x._2))
      .filter(x => x._1 != Nil) // filter out entries that do not join anywhere
      .flatMap(t => t._1.map(s => (s._2, t._2))) // create pairs of (index1, rdd2Elem)
      .groupByKey()

    indexedRdd1 // this is RDD[(Int, (Interval[Int], Row))]
      .map(x => (x._1, x._2._2)) // convert it to (Int, Row)
      .join(kvrdd2) // join produces RDD[(Int, (Row, Iterable[Row]))]
      .map(_._2) // end up with RDD[(Row, Iterable[Row])]
  }

}
