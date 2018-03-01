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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, _}
import org.apache.spark.sql.internal.SQLConf
import org.biodatageeks.rangejoins.IntervalTree.{Interval, IntervalTreeJoinOptimImpl, IntervalWithRow}

@DeveloperApi
case class IntervalTreeJoinOptimChromosome(left: SparkPlan,
                                           right: SparkPlan,
                                           condition: Seq[Expression],
                                           context: SparkSession, leftLogicalPlan: LogicalPlan,
                                           rightLogicalPlan: LogicalPlan,
                                           minOverlap: Int, maxGap: Int, maxBroadcastSize : Int) extends BinaryExecNode {
  def output = left.output ++ right.output

  lazy val (buildPlan, streamedPlan) = (left, right)

  lazy val (buildKeys, streamedKeys) = (List(condition(0), condition(1),condition(4)),
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
    val v1Size = {
     if(ifStatsAvailable(leftLogicalPlan,conf))
       leftLogicalPlan.stats(conf).sizeInBytes.toLong
      else
        v1.count
    }

    val v2Size = {if(ifStatsAvailable(rightLogicalPlan,conf))
      rightLogicalPlan.stats(conf).sizeInBytes.toLong
    else
      v2.count
    }
    if ( v1Size < v2Size ) {
      val v3 = IntervalTreeJoinOptimChromosomeImpl.overlapJoin(context.sparkContext, v1kv, v2kv,
        if (!ifStatsAvailable(leftLogicalPlan,conf)) v1.count() else v1Size, minOverlap, maxGap, maxBroadcastSize) //FIXME:can be further optimized!
     v3.mapPartitions(
       p => {
         val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
         p.map(r=>joiner.join(r._1.asInstanceOf[UnsafeRow],r._2.asInstanceOf[UnsafeRow]))
       }

     )

    }
    else {
      val v3 = IntervalTreeJoinOptimChromosomeImpl.overlapJoin(context.sparkContext, v2kv, v1kv,
        if(!ifStatsAvailable(rightLogicalPlan,conf)) v2.count() else v2Size, minOverlap, maxGap, maxBroadcastSize)
      v3.mapPartitions(
        p => {
          val joiner = GenerateUnsafeRowJoiner.create(right.schema, left.schema)
          p.map(r=>joiner.join(r._2.asInstanceOf[UnsafeRow],r._1.asInstanceOf[UnsafeRow]))
        }

      )
    }

  }

  private def ifStatsAvailable(logicalPlan: LogicalPlan, conf :SQLConf) = {
    (logicalPlan
      .stats(conf)
      .sizeInBytes != Long.MaxValue)
  }
}
