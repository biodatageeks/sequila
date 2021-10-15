package org.biodatageeks.sequila.pileup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.sequila.datasources.BAM.BAMTableReader
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.model.AlignmentsRDDOperations.implicits._
import org.biodatageeks.sequila.utils.InternalParams
import org.seqdoop.hadoop_bam.BAMBDGInputFormat
import org.slf4j.LoggerFactory

class Pileup(spark:SparkSession) {
  val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  def handlePileup(tableName: String, sampleId: String, refPath:String, output: Seq[Attribute], conf: Conf): RDD[InternalRow] = {
    logger.info(s"Calculating pileup on table: $tableName with configuration\n$conf")

    val bdConf = spark.sparkContext.broadcast(conf)

    val tableReader = new BAMTableReader[BAMBDGInputFormat](spark, tableName, sampleId)
    val (alignments, bounds) =
      tableReader
      .readFile // all alignments from file
      .filterByConfig(bdConf, spark) // filtered with user defined config
      .repartition(tableReader, conf)

    PileupMethods.calculatePileup(alignments, bounds, spark, refPath, bdConf)
  }
}
