package  org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Range, Statistics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.biodatageeks.sequila.utils.Columns

case class GenomicInterval(
                            contig:String,
                            start:Int,
                            end:Int,
                            output: Seq[Attribute]
                          )  extends LeafNode with MultiInstanceRelation with Serializable {

  override def newInstance(): GenomicInterval = copy(output = output.map(_.newInstance()))

  def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = IntegerType.defaultSize * 2 //FIXME: Add contigName size
    Statistics( sizeInBytes = sizeInBytes )
  }

  override def toString: String = {
    s"GenomicInterval ($contig, $start, $end)"
  }

}
/** Factory for constructing new `GenomicInterval` nodes. */
object GenomicInterval {
  def apply(contig:String, start: Int, end: Int): GenomicInterval = {
    val output = StructType(Seq(
      StructField(s"${Columns.CONTIG}", StringType, nullable = false),
      StructField(s"${Columns.START}", IntegerType, nullable = false),
      StructField(s"${Columns.END}", IntegerType, nullable = false))
    )  .toAttributes
    new GenomicInterval(contig,start, end, output)
  }
}



