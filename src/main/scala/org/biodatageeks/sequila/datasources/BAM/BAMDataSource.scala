package org.biodatageeks.sequila.datasources.BAM

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources._
import org.biodatageeks.sequila.utils.{FastSerializer, InternalParams, TableFuncs}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat


class BAMDataSource extends DataSourceRegister
  with RelationProvider
  with CreatableRelationProvider
  //with InsertableRelation
  with BDGAlignFileReaderWriter[BAMBDGInputFormat] {
  override def shortName(): String = "BAM"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    new BDGAlignmentRelation[BAMBDGInputFormat](parameters("path"))(sqlContext)
  }



  def save(spark: SparkSession, parameters: Map[String, String], mode: SaveMode, data: DataFrame) = {

    import spark.implicits._
    val ds = data
      .as[BDGSAMRecord]
    val sampleName = ds.first().sampleId
    val samplePath = s"${parameters(InternalParams.BAMCTASDir)}/${sampleName}*.bam"

    val outPathString = s"${parameters("path").split('/').dropRight(1).mkString("/")}/${sampleName}.bam"
    val outPath = new Path(outPathString)
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var fos:FSDataOutputStream  = null

    if (hdfs.exists(outPath) && mode == SaveMode.Overwrite) {
      hdfs.delete(outPath,true)
      fos = hdfs.create(outPath)
    }
    else if(!hdfs.exists(outPath)) { //
      fos = hdfs.create(outPath)
    }
    else
      throw new Exception(s"Path: ${outPathString} already exits and saveMode is not ${SaveMode.Overwrite}")

    fos.close()

    //FIXME: Try to get rid of ser/deser if objects could be serialized the way BAMBDGOutputFormat writes it back
    //can be potentially orders of magnitude faster
    //see https://github.com/samtools/htsjdk/blob/master/src/main/java/htsjdk/samtools/BAMRecordCodec.java
    //https://github.com/YTLogos/gatk/blob/master/src/main/java/htsjdk/samtools/SAMRecordSparkCodec.java

    val srcBAMRDD =
      ds
       .rdd
      .mapPartitions(p =>{
        val bdgSerializer = new FastSerializer()
        p.map( r => bdgSerializer.fst.asObject(r.SAMRecord.get).asInstanceOf[SAMRecord] )
      })
    val headerPath = TableFuncs.getExactSamplePath(spark,samplePath)
    saveAsBAMFile(spark.sqlContext,srcBAMRDD,outPathString,headerPath)
  }

  //CTAS
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {

    sqlContext.setConf(InternalParams.BAMCTASCmd,"true")
    val spark = sqlContext.sparkSession
    save(spark, parameters, mode ,data)
    //revert to default after saving records
    sqlContext.setConf(InternalParams.BAMCTASCmd,"false")
    createRelation(sqlContext, parameters)
  }

//  //IAS
//  override def insert(data: DataFrame, overwrite: Boolean) = {
//      print("DS")
//  }

}