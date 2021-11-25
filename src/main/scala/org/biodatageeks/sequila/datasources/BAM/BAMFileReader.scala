package org.biodatageeks.sequila.datasources.BAM

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat

import scala.reflect.ClassTag

class BAMFileReader [T<:BDGAlignInputFormat] (spark:SparkSession, path: String, refPath: Option[String]) (implicit c: ClassTag[T])
  extends BDGAlignFileReaderWriter[T]{

  override
  def readFile: RDD[SAMRecord] = {
    readBAMFile(spark.sqlContext, path, refPath)
  }

}
