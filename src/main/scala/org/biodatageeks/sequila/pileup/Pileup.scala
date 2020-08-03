package org.biodatageeks.sequila.pileup

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.utils.{InternalParams, TableFuncs}
import org.seqdoop.hadoop_bam.CRAMBDGInputFormat
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag


class Pileup[T<:BDGAlignInputFormat](spark:SparkSession)(implicit c: ClassTag[T]) extends BDGAlignFileReaderWriter[T] {
  val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  def handlePileup(tableName: String, sampleId: String, refPath:String, output: Seq[Attribute]): RDD[InternalRow] = {
    logger.info(s"### Calculating pileup on table: $tableName with quals set = ${Conf.includeBaseQualities}")

    lazy val allAlignments = readTableFile(name=tableName, sampleId)

    if(logger.isDebugEnabled()) logger.debug("Processing {} reads in total", allAlignments.count() )

    val alignments = filterAlignments(allAlignments )

    PileupMethods.calculatePileup(alignments, spark, refPath)

  }

  private def filterAlignments(alignments:RDD[SAMRecord]): RDD[SAMRecord] = {
    // any other filtering conditions should go here
    val filterFlag = spark.conf.get(InternalParams.filterReadsByFlag, Conf.filterFlag).toInt
    val cleaned = alignments.filter(read => read.getContig != null && (read.getFlags & filterFlag) == 0)
    if(logger.isDebugEnabled()) logger.debug("Processing {} cleaned reads in total", cleaned.count() )
    cleaned
  }

  private def readTableFile(name: String, sampleId: String): RDD[SAMRecord] = {
    val metadata = TableFuncs.getTableMetadata(spark, name)
    val path = metadata.location.toString

    val samplePathTemplate = (
      path
      .split('/')
      .dropRight(1) ++ Array(s"$sampleId*.{{fileExtension}}"))
      .mkString("/")

    metadata.provider match {
      case Some(f) =>
        if (f == InputDataType.BAMInputDataType)
           readBAMFile(spark.sqlContext, samplePathTemplate.replace("{{fileExtension}}", "bam"), refPath = None)
        else if (f == InputDataType.CRAMInputDataType) {
          val refPath = spark.sqlContext
            .sparkContext
            .hadoopConfiguration
            .get(CRAMBDGInputFormat.REFERENCE_SOURCE_PATH_PROPERTY)
           readBAMFile(spark.sqlContext, samplePathTemplate.replace("{{fileExtension}}", "cram"), Some(refPath))
        }
        else throw new Exception("Only BAM and CRAM file formats are supported in bdg_coverage.")
      case None => throw new Exception("Wrong file extension - only BAM and CRAM file formats are supported in bdg_coverage.")
    }
  }
}
