package org.biodatageeks.sequila.datasources.BAM

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources._
import org.biodatageeks.sequila.utils.{FastSerializer, InternalParams, TableFuncs}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat


class BAMDataSource extends DataSourceRegister
  with RelationProvider
  with BDGAlignFileReaderWriter[BAMBDGInputFormat] {
  override def shortName(): String = "bam"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    new BDGAlignmentRelation[BAMBDGInputFormat](parameters("path"))(sqlContext)
  }


}