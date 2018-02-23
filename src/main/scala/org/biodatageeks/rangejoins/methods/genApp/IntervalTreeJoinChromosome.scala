package org.biodatageeks.rangejoins.methods.genApp

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Expression, InterpretedProjection, UnsafeRow}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.biodatageeks.rangejoins.genApp.Interval

@DeveloperApi
case class
IntervalTreeJoinChromosome(left: SparkPlan,
                             right: SparkPlan,
                             condition: Seq[Expression],
                             context: SparkSession) extends BinaryExecNode {
  def output = left.output ++ right.output

  lazy val (buildPlan, streamedPlan) = (left, right)

  lazy val (buildKeys, streamedKeys) = (List(condition(0), condition(1),condition(4)),
    List(condition(2), condition(3),condition(5)))

  @transient lazy val buildKeyGenerator = new InterpretedProjection(buildKeys, buildPlan.output)
  @transient lazy val streamKeyGenerator = new InterpretedProjection(streamedKeys,
    streamedPlan.output)

  protected override def doExecute(): RDD[InternalRow] = {
    val v1 = left.execute()
    val v1kv = v1.map(x => {
      val v1Key = buildKeyGenerator(x)

      ((v1Key.getString(2),new Interval[Int](v1Key.getInt(0), v1Key.getInt(1))),
        x.copy())
    })
    val v2 = right.execute()
    val v2kv = v2.map(x => {
      val v2Key = streamKeyGenerator(x)
      ((v2Key.getString(2),new Interval[Int](v2Key.getInt(0), v2Key.getInt(1))),
        x.copy())
    })
    /* As we are going to collect v1 and build an interval tree on its intervals,
    make sure that its size is the smaller one. */
    if (v1.count <= v2.count) {
      val v3 = IntervalTreeJoinChromosomeImpl.overlapJoin(context.sparkContext, v1kv, v2kv)
        .flatMap(l => l._2
          .map(r => (l._1, r)))
      v3.map {
        case (l: InternalRow, r: InternalRow) => {
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema);
          joiner.join(l.asInstanceOf[UnsafeRow], r.asInstanceOf[UnsafeRow]).asInstanceOf[InternalRow] //resultProj(joinedRow(l, r)) joiner.joiner
        }
      }
    }
    else {
      val v3 = IntervalTreeJoinChromosomeImpl.overlapJoin(context.sparkContext, v2kv, v1kv).flatMap(l => l._2.map(r => (l._1, r)))
      v3.map {
        case (r: InternalRow, l: InternalRow) => {
          val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema);
          joiner.join(l.asInstanceOf[UnsafeRow], r.asInstanceOf[UnsafeRow]).asInstanceOf[InternalRow] //resultProj(joinedRow(l, r)) joiner.joiner
        }
      }
    }

  }
}
