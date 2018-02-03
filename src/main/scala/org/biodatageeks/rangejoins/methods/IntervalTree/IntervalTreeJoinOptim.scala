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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.{SparkPlan, _}

@DeveloperApi
case class IntervalTreeJoinOptim(left: SparkPlan,
                                 right: SparkPlan,
                                 condition: Seq[Expression],
                                 context: SparkSession) extends BinaryExecNode {
  def output = left.output ++ right.output

  lazy val (buildPlan, streamedPlan) = (left, right)

  lazy val (buildKeys, streamedKeys) = (List(condition(0), condition(1)),
    List(condition(2), condition(3)))

  @transient lazy val buildKeyGenerator = new InterpretedProjection(buildKeys, buildPlan.output)
  @transient lazy val streamKeyGenerator = new InterpretedProjection(streamedKeys,
    streamedPlan.output)

  protected override def doExecute(): RDD[InternalRow] = {
    val v1 = left.execute()
    val v1kv = v1.map(x => {
      val v1Key = buildKeyGenerator(x)

      (new IntervalWithRow[Int](v1Key.getInt(0), v1Key.getInt(1),
        x) )

    })
    val v2 = right.execute()
    val v2kv = v2.map(x => {
      val v2Key = streamKeyGenerator(x)
      (new IntervalWithRow[Int](v2Key.getInt(0), v2Key.getInt(1),
        x) )
    })
    /* As we are going to collect v1 and build an interval tree on its intervals,
    make sure that its size is the smaller one. */
    if (v1.count <= v2.count) {
      val v3 = IntervalTreeJoinOptimImpl.overlapJoin(context.sparkContext, v1kv, v2kv)
     v3.mapPartitions(
       p => {
         val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
         p.map(r=>joiner.join(r._1.asInstanceOf[UnsafeRow],r._2.asInstanceOf[UnsafeRow]))
       }



     )

    }
    else {
      val v3 = IntervalTreeJoinOptimImpl.overlapJoin(context.sparkContext, v2kv, v1kv)
      v3.mapPartitions(
        p => {
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
          p.map(r=>joiner.join(r._2.asInstanceOf[UnsafeRow],r._1.asInstanceOf[UnsafeRow]))
        }

      )
    }

  }
}
