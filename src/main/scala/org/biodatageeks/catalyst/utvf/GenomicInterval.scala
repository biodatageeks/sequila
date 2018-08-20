package  org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Range, Statistics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

case class GenomicInterval(
                          contigName:String,
                          start:Int,
                          end:Int,
                          output: Seq[Attribute]
                          )  extends LeafNode with MultiInstanceRelation with Serializable {

  override def newInstance(): GenomicInterval = copy(output = output.map(_.newInstance()))

  override def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = IntegerType.defaultSize * 2 //FIXME: Add contigName size
    Statistics( sizeInBytes = sizeInBytes )
  }

  override def simpleString: String = {
    s"GenomicInterval ($contigName, $start, $end)"
  }

}
/** Factory for constructing new `GenomicInterval` nodes. */
object GenomicInterval {
  def apply(contigName:String, start: Int, end: Int): GenomicInterval = {
    val output = StructType(Seq(
      StructField("contigName", StringType, nullable = false),
      StructField("start", IntegerType, nullable = false),
      StructField("end", IntegerType, nullable = false))
    )  .toAttributes
    new GenomicInterval(contigName,start, end, output)
  }
}



