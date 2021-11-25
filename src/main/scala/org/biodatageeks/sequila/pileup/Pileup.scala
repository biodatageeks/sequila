package org.biodatageeks.sequila.pileup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.sequila.datasources.BAM.{BAMFileReader, BAMTableReader}
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.model.AlignmentsRDDOperations.implicits._
import org.biodatageeks.sequila.utils.{FileFuncs, TableFuncs}
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, CRAMBDGInputFormat}
import org.slf4j.LoggerFactory

class Pileup(spark:SparkSession) {
  val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  def handlePileup(tableNameOrPath: String, sampleId: String, refPath:String, output: Seq[Attribute], conf: Conf): RDD[InternalRow] = {

    if(sampleId != null)
      logger.info(s"Calculating pileup on table: $tableNameOrPath with configuration\n$conf")
    else
      logger.info(s"Calculating pileup using file: $tableNameOrPath with configuration\n$conf")
    val bdConf = spark.sparkContext.broadcast(conf)

    val (alignments, bounds) = {
      if (sampleId != null) {
        val metadata = TableFuncs.getTableMetadata(spark, tableNameOrPath)
        val tableReader = metadata.provider match {
          case Some(f) if sampleId != null =>
            if (f == InputDataType.BAMInputDataType)
              new BAMTableReader[BAMBDGInputFormat](spark, tableNameOrPath, sampleId, "bam", None)
            else if (f == InputDataType.CRAMInputDataType) {
              new BAMTableReader[CRAMBDGInputFormat](spark, tableNameOrPath, sampleId, "cram", Some(refPath))
            }
            else throw new Exception("Only BAM and CRAM file formats are supported.")
          case None => throw new Exception("Empty file extension - BAM and CRAM file formats are supported.")
        }
        tableReader
          .readFile // all alignments from file
          .filterByConfig(bdConf, spark)
          .repartition(tableReader.asInstanceOf[BAMTableReader[BDGAlignInputFormat]], conf)

      }
      else {
        val fileReader = FileFuncs.getFileExtension(tableNameOrPath) match {
          case "bam" => new BAMFileReader[BAMBDGInputFormat](spark, tableNameOrPath, None)
          case "cram" => new BAMFileReader[CRAMBDGInputFormat](spark, tableNameOrPath, Some(refPath))
        }
        fileReader
          .readFile
          .filterByConfig(bdConf, spark)
          .repartition(fileReader.asInstanceOf[BAMFileReader[BDGAlignInputFormat]], conf)
      }
    }
    PileupMethods.calculatePileup(alignments, bounds, spark, refPath, bdConf, output)
  }
}
