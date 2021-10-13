package org.biodatageeks.sequila.datasources.BAM

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.utils.TableFuncs
import org.seqdoop.hadoop_bam.CRAMBDGInputFormat
import scala.reflect.ClassTag

class BAMTableReader [T<:BDGAlignInputFormat] (spark:SparkSession, name: String, sampleId: String) (implicit c: ClassTag[T]) extends BDGAlignFileReaderWriter[T] {
  def readFile: RDD[SAMRecord] = {
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
        else throw new Exception("Only BAM and CRAM file formats are supported.")
      case None => throw new Exception("Wrong file extension - only BAM and CRAM file formats are supported.")
    }
  }

}
