package org.biodatageeks.datasources.BAM

import htsjdk.samtools.{SAMRecord, ValidationStringency}
import org.apache.hadoop.io.LongWritable
import org.apache.log4j.Logger
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}

import scala.collection.mutable.ArrayBuffer


case class BAMRecord(sampleId: String,
                     contigName:String,
                     start:Int,
                     end:Int,
                     cigar:String,
                     mapq:Int,
                     baseq: String,
                     reference:String,
                     flags:Int,
                     materefind:Int)

class BAMRelation (path:String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with Serializable {


  val spark = sqlContext
    .sparkSession

  val columnNames = Array(
    "sampleId",
    "contigName",
    "start",
    "end",
    "cigar",
    "mapq",
    "baseq",
    "reference",
    "flags",
    "materefind"
  )

  spark
    .sparkContext
    .hadoopConfiguration
    .set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)

  override def schema: org.apache.spark.sql.types.StructType = {
    StructType(
      Seq(
        new StructField(columnNames(0), StringType),
        new StructField(columnNames(1), StringType),
        new StructField(columnNames(2), IntegerType),
        new StructField(columnNames(3), IntegerType),
        new StructField(columnNames(4), StringType),
        new StructField(columnNames(5), IntegerType),
        new StructField(columnNames(6), StringType),
        new StructField(columnNames(7), StringType),
        new StructField(columnNames(8), IntegerType),
        new StructField(columnNames(9), IntegerType)

      )
    )
  }

  override def buildScan(requiredColumns:Array[String], filters:Array[Filter]): RDD[Row] = {


    val samples = ArrayBuffer[String]()

    filters.foreach(f=>

      f match {
        case EqualTo(attr, value) => {
          if (attr.toLowerCase == "sampleid" || attr.toLowerCase == "sample_id")

            samples+=value.toString
        }
        case In(attr, values) => {
          if (attr.toLowerCase == "sampleid" || attr.toLowerCase == "sample_id"){
            values.foreach(s=> samples+=s.toString)
          }
        }
        case _ => None
      }


    )
    val prunedPaths = if(samples.isEmpty) {
      path
    }
    else{
      val parent = path.split('/').dropRight(1)
      samples.map{
        s => s"${parent.mkString("/")}/${s}*.bam"
      }
        .mkString(",")
    }
    val logger =  Logger.getLogger(this.getClass.getCanonicalName)
    if(prunedPaths != path) logger.warn(s"Partition pruning detected, reading only files for samples: ${samples.mkString(",")}")
    val alignments = spark
      .sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](prunedPaths)
    val alignmentsWithFileName = alignments.asInstanceOf[NewHadoopRDD[LongWritable, SAMRecordWritable]]
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
        val file = inputSplit.asInstanceOf[FileVirtualSplit]
        iterator.map(tup => (file.getPath.getName.split('.')(0), tup._2))
        }
      )
    val sampleAlignments = alignmentsWithFileName
      .map(r => (r._1, r._2.get()))
      .map { case (sampleId, r) =>
          val record = new Array[Any](requiredColumns.length)
          //requiredColumns.
          for(i<- 0 to requiredColumns.length-1){
            record(i) = getValueFromColumn(requiredColumns(i),r,sampleId)
          }
          Row.fromSeq(record)
      }

    sampleAlignments


  }

  private def getValueFromColumn(colName:String,r:SAMRecord, sampleId:String): Any = {

    if(colName == columnNames(0)) sampleId
    else if (colName == columnNames(1)) r.getContig
    else if (colName == columnNames(2)) r.getStart
    else if (colName == columnNames(3)) r.getEnd
    else if (colName == columnNames(4)) r.getCigar.toString
    else if (colName == columnNames(5)) r.getMappingQuality
    else if (colName == columnNames(6)) r.getBaseQualityString
    else if (colName == columnNames(7)) r.getReferenceName
    else if (colName == columnNames(8)) r.getFlags
    else if (colName == columnNames(9)) r.getMateReferenceIndex
    else throw new Exception("Unknowe column")

  }
}