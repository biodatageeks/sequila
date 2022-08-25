package org.biodatageeks.sequila.rangejoins.exp

import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.biodatageeks.sequila.utils.InternalParams
import org.rogach.scallop.{ScallopConf, ScallopOption}

object SparkSingleClassSQLTest {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val intervalHolderClass: ScallopOption[String] = opt[String](required = false,
      default = Some("org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeRedBlack"))
    val treeTablePath: ScallopOption[String] = opt[String](required = true)
    val datasetTablePath: ScallopOption[String] = opt[String](required = true)
    val domainsNum: ScallopOption[String] = opt[String](required = false)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local[1]")
        .getOrCreate()
    }
    println(s"applicationId=${spark.sparkContext.applicationId}")
    val ss = SequilaSession(spark)
    spark.sparkContext.setLogLevel("INFO")
    SequilaSession.register(ss)
    ss.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dataset_tab (contig STRING, pos_start INT, pos_end INT)
         |USING CSV LOCATION '${conf.datasetTablePath()}'
      """.stripMargin)
    ss.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS tree_tab (contig STRING, pos_start INT, pos_end INT)
         |USING CSV LOCATION '${conf.treeTablePath()}'
      """.stripMargin)


    ss.sqlContext.setConf(InternalParams.useJoinOrder, "true")
    ss.sqlContext.setConf(InternalParams.intervalHolderClass, conf.intervalHolderClass())
    if (conf.domainsNum.isDefined) {
      ss.sqlContext.setConf(InternalParams.domainsNum, conf.domainsNum())
    }
    ss.sqlContext.setConf(InternalParams.maxBroadCastSize, (6000L * 1024 * 1024).toString)
    val query =
      s"""
         | SELECT count(*) as cnt
         | FROM dataset_tab AS t1
         | JOIN tree_tab AS t2 ON
         | t1.contig = t2.contig AND
         | t2.pos_end >= t1.pos_start AND
         | t2.pos_start <= t1.pos_end""".stripMargin

    val q = ss
      .sql(query)
    q.show
  }
}
