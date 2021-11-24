package org.biodatageeks.sequila.datasources.BAM

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.utils.TableFuncs
import org.seqdoop.hadoop_bam.CRAMBDGInputFormat
import scala.reflect.ClassTag

class BAMTableReader [T<:BDGAlignInputFormat] (spark:SparkSession, name: String,
                                               sampleId: String, fileExt: String, refPath: Option[String]) (implicit c: ClassTag[T])
  extends BDGAlignFileReaderWriter[T] {

  override
  def readFile: RDD[SAMRecord] = {
    val metadata = TableFuncs.getTableMetadata(spark, name)
    val path = metadata.location.toString
    val samplePathTemplate = (
      path
        .split('/')
        .dropRight(1) ++ Array(s"$sampleId*.{{fileExtension}}"))
      .mkString("/")
    readBAMFile(spark.sqlContext, samplePathTemplate.replace("{{fileExtension}}", fileExt), refPath = refPath)
  }

}
