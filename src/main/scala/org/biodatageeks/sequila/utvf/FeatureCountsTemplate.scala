package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.sequila.utils.Columns

case class FeatureCountsTemplate(reads: String,
                                 genes: String,
                                 output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation with Serializable {
  override def newInstance(): FeatureCountsTemplate = copy(output = output.map(_.newInstance()))

  override def toString: String = {
    s"FeatureCounts ($reads, $genes)"
  }
}

object FeatureCountsTemplate {
  def apply(reads: String, genes: String): FeatureCountsTemplate = {
    val output = StructType(Seq(
      StructField(s"${Columns.SAMPLE}", StringType, nullable = false),
      StructField(s"${Columns.CONTIG}", StringType, nullable = false),
      StructField(s"${Columns.START}", IntegerType, nullable = false),
      StructField(s"${Columns.END}", IntegerType, nullable = false),
      StructField(s"${Columns.STRAND}", StringType, nullable = true),
      StructField(s"${Columns.LENGTH}", IntegerType, nullable = false))
    ).toAttributes
    new FeatureCountsTemplate(reads, genes, output)
  }
}