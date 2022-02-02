package org.biodatageeks.sequila.tests.rangejoins

import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.formats.BrowserExtensibleData
import org.biodatageeks.sequila.rangejoins.IntervalTree.Interval
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalHolderChromosome
import org.biodatageeks.sequila.utils.InternalParams
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `iterator asScala`}


object BamBedChecker {
  def overlapsCount(spark: SparkSession, bedFilePath: String, classHolder: String) : Long =  {
    val ss = SequilaSession(spark)

    val tableNameBAM = "tab1"
    val path = com.google.common.io.Resources.getResource("multichrom/mdbam/NA12878.multichrom.md.bam").getPath
    val tab2 = "tab2"
    val path2 = com.google.common.io.Resources.getResource(bedFilePath).getPath
    spark
      .sqlContext.setConf(InternalParams.useJoinOrder, "true")
    spark
      .sqlContext.setConf(InternalParams.intervalHolderClass,
      Class.forName(classHolder).getName)

    ss.sql(s"""DROP  TABLE IF  EXISTS $tableNameBAM""")
    ss.sql(
      s"""
         |CREATE TABLE $tableNameBAM
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$path")
         |
      """.stripMargin)

    ss.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $tab2
         |USING org.biodatageeks.sequila.datasources.BED.BEDDataSource
         |OPTIONS(path "$path2")
         |
      """.stripMargin)

    import spark.implicits._

    val ds1 = ss
      .sql(s"SELECT * FROM $tableNameBAM")
      .as[BAMData]

    val ds2 = ss
      .sql(s"SELECT * FROM $tab2")
      .as[BrowserExtensibleData]

    val localIntervals = ds1
      .rdd
      .map(r => ("1", Interval[Int](r.pos_start, r.pos_end), InternalRow.empty))
      .collect()

    val tree = new IntervalHolderChromosome[InternalRow](localIntervals, classHolder)
    val intervalTree = spark.sparkContext.broadcast(tree)

    val cnt = ds2.map(r => intervalTree.value.getIntervalTreeByChromosome(r.contig) match {
      case Some(t) => {
        val record = t.overlappers(r.pos_start, r.pos_end).toList
        record
          .flatMap(k => k.getValue.map(_ => 1))
      }
      case _ => List.empty
    })
      .flatMap(r => r)
      .count()
    println(cnt)
    cnt
  }
}
final case class BAMData(pos_start: Int, pos_end: Int)
