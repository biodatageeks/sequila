package org.biodatageeks.sequila.utvf

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{DataFrame, GenomicInterval, SparkSession, Strategy}
import org.apache.spark.unsafe.types.UTF8String

case class GIntervalRow(contigName: String, start: Int, end: Int)
class GenomicIntervalStrategy( spark: SparkSession) extends Strategy with Serializable  {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case GenomicInterval(contigName, start, end,output) => GenomicIntervalPlan(plan,spark,GIntervalRow(contigName,start,end),output) :: Nil
    case _ => Nil

  }
}

case class GenomicIntervalPlan(plan: LogicalPlan, spark: SparkSession,interval:GIntervalRow, output: Seq[Attribute]) extends SparkPlan with Serializable {
  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    import spark.implicits._

    lazy val genomicInterval = spark.createDataset(Seq(interval))
    genomicInterval
        .rdd
      .map(r=>{
        val proj =  UnsafeProjection.create(schema)
        proj.apply(InternalRow.fromSeq(Seq(UTF8String.fromString(r.contigName),r.start,r.end)))
        }
      )
  }
  def children: Seq[SparkPlan] = Nil

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = this
}
