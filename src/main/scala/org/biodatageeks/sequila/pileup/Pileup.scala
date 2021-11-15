package org.biodatageeks.sequila.pileup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.sequila.datasources.BAM.BAMTableReader
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.model.AlignmentsRDDOperations.implicits._
import org.biodatageeks.sequila.utils.TableFuncs
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, CRAMBDGInputFormat}
import org.slf4j.LoggerFactory

class Pileup(spark:SparkSession) {
  val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  def handlePileup(tableName: String, sampleId: String, refPath:String, output: Seq[Attribute], conf: Conf): RDD[InternalRow] = {
    logger.info(s"Calculating pileup on table: $tableName with configuration\n$conf")

    val bdConf = spark.sparkContext.broadcast(conf)
    val metadata = TableFuncs.getTableMetadata(spark, tableName)
    val tableReader = metadata.provider match {
      case Some(f) =>
        if (f == InputDataType.BAMInputDataType)
          new BAMTableReader[BAMBDGInputFormat](spark, tableName, sampleId, "bam", None)
        else if (f == InputDataType.CRAMInputDataType) {
          new BAMTableReader[CRAMBDGInputFormat](spark, tableName, sampleId, "cram", Some(refPath))
        }
        else throw new Exception("Only BAM and CRAM file formats are supported.")
      case None => throw new Exception("Empty file extension - BAM and CRAM file formats are supported.")
    }
    val (alignments, bounds) =
      tableReader
      .readFile // all alignments from file
      .filterByConfig(bdConf, spark) // filtered with user defined config
      .repartition(tableReader.asInstanceOf[BAMTableReader[BDGAlignInputFormat]], conf)

    PileupMethods.calculatePileup(alignments, bounds, spark, refPath, bdConf)
  }
}
