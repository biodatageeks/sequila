package org.biodatageeks.rangejoins.generation

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Random

object TestDataGenerator {
  def main(args: Array[String]): Unit = {
    val queryPath = "generated/query"
    val labelPath = "generated/label"
    val spark = SparkSession.builder().master("local[*]").appName("generator").getOrCreate()
    val query = generateRangesList(1000, 50, 5,30).map(rr => QueryRecord(rr.start, rr.end, s"q${rr.index}"))
    val label = generateRangesList(1000, 50, 15,20).map(rr => LabelRecord(rr.start, rr.end, s"l${rr.index}"))
    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(query).toDF.write.csv(queryPath)
    spark.sparkContext.parallelize(label).toDF.write.csv(labelPath)
    // brute force join
    val joined = query.map(q=> (q,label.filter(l => q.end >= l.start && q.start <= l.end)))
      .map(e=> if(e._2.isEmpty) (e._1, Seq(LabelRecord(0,0,null)).toList) else e)
      .flatMap(e=> e._2.map(l=>(e._1, l)))
      .map(e=>s"${e._1.start},${e._1.end},${e._1.value},${e._2.start},${e._2.end},${e._2.value}")
    spark.sparkContext.parallelize(joined).saveAsTextFile("generated/result")
  }

  def generateRangesList(amount: Int, maxOffset: Int, maxRange: Int, maxStep: Int): Seq[RangeRecord] = {
    val r = new Random()
    var offset = r.nextInt(maxOffset)
    var list = new mutable.MutableList[RangeRecord]()
    for (i <- Range(0, amount)) {
      val size = r.nextInt(maxRange - 1) + 1
      val step = r.nextInt(maxStep - 1) + 1
      list += RangeRecord(offset, offset+size, i)
      offset = offset + size + step
    }
    list
  }
}

case class RangeRecord(start: Int, end: Int, index: Int)

case class QueryRecord(start: Int, end: Int, value: String)

case class LabelRecord(start: Int, end: Int, value: String)
