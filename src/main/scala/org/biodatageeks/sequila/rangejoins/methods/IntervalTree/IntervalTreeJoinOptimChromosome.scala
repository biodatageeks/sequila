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

package org.biodatageeks.sequila.rangejoins.methods.IntervalTree

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, _}
import org.apache.spark.sql.internal.SQLConf
import org.biodatageeks.sequila.rangejoins.IntervalTree.Interval
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinOptimImpl.logger

@DeveloperApi
case class IntervalTreeJoinOptimChromosome(left: SparkPlan,
                                           right: SparkPlan,
                                           condition: Seq[Expression],
                                           context: SparkSession,
                                           minOverlap: Int, maxGap: Int,
                                           useJoinOrder: Boolean,
                                           intervalHolderClassName: String
                                          ) extends BinaryExecNode with Serializable {
  @transient lazy val output = left.output ++ right.output

  @transient lazy val (buildPlan, streamedPlan) = (left, right)

  @transient lazy val (buildKeys, streamedKeys) = (List(condition(0), condition(1),condition(4)),
    List(condition(2), condition(3),condition(5)))

  @transient lazy val buildKeyGenerator = new InterpretedProjection(buildKeys, buildPlan.output)
  @transient lazy val streamKeyGenerator = new InterpretedProjection(streamedKeys, streamedPlan.output)

  protected override def doExecute(): RDD[InternalRow] = {
    val v1 = left.execute()
    val v1kv = v1.map(x => {
      val v1Key = buildKeyGenerator(x)
      (v1Key.getString(2), (new Interval[Int](v1Key.getInt(0), v1Key.getInt(1))),
        x )

    })
    val v2 = right.execute()
    val v2kv = v2.map(x => {
      val v2Key = streamKeyGenerator(x)
      (v2Key.getString(2), (new Interval[Int](v2Key.getInt(0), v2Key.getInt(1))),
        x )
    })
    /* As we are going to collect v1 and build an interval tree on its intervals,
    make sure that its size is the smaller one. */

    val conf = new SQLConf()
    val v1Size = if(useJoinOrder) Long.MaxValue else v1.count
//    val v1Size = {
//     if(ifStatsAvailable(leftLogicalPlan,conf))
//       leftLogicalPlan.stats(conf).sizeInBytes.toLong
//      else
//        v1.count
//    }

//    val v2Size = {if(ifStatsAvailable(rightLogicalPlan,conf))
//      rightLogicalPlan.stats(conf).sizeInBytes.toLong
//    else
//      v2.count
//    }

    val v2Size = if(useJoinOrder) Long.MinValue else v2.count()

    if ( v1Size < v2Size ) {
      logger.info(s"Broadcasting first table")
      val v3 = IntervalTreeJoinOptimChromosomeImpl.overlapJoin(context, v1kv, v2kv,
         v1.count(), minOverlap, maxGap, intervalHolderClassName) //FIXME:can be further optimized!
     v3.mapPartitions(
       p => {
         val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
         p.map(r=>joiner.join(r._1.asInstanceOf[UnsafeRow],r._2.asInstanceOf[UnsafeRow]))
       }

     )

    }
    else {
      logger.info(s"Broadcasting second table")
      val v3 = IntervalTreeJoinOptimChromosomeImpl.overlapJoin(context, v2kv, v1kv,
         v2.count(), minOverlap, maxGap, intervalHolderClassName)
      v3.mapPartitions(
        p => {
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
          p.map(r=>joiner.join(r._2.asInstanceOf[UnsafeRow],r._1.asInstanceOf[UnsafeRow]))
        }

      )
    }

  }

  private def ifStatsAvailable(logicalPlan: LogicalPlan, conf :SQLConf) = {
    (logicalPlan
      .stats
      .sizeInBytes != Long.MaxValue)
  }

  override protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan  =  copy(left = newLeft, right = newRight)
}
