package org.biodatageeks.sequila.datasources.FASTQ

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.log4j.Logger
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.{Encoders, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, PrunedFilteredScan}
import org.biodatageeks.formats.SequencedFragment
import org.biodatageeks.sequila.utils.{Columns, FastSerializer}
import org.seqdoop.hadoop_bam.{FastqInputFormat, FileVirtualSplit, SequencedFragment}

import scala.collection.mutable.ArrayBuffer

class SequencedFragmentRelation(path: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with Serializable {

  @transient val logger = Logger.getLogger(this.getClass.getCanonicalName)


  override def schema: org.apache.spark.sql.types.StructType = Encoders.product[org.biodatageeks.formats.SequencedFragment].schema

  override def buildScan(requiredColumns:Array[String], filters:Array[Filter]): RDD[Row] = {
    val samples = ArrayBuffer[String]()

    filters.foreach(f => {
      f match {
        case EqualTo(attr, value) => {
          if (attr.equalsIgnoreCase(Columns.SAMPLE))
            samples += value.toString
        }
        case In(attr, values) => {
          if (attr.equalsIgnoreCase(Columns.SAMPLE) ) {
            values.foreach(s => samples += s.toString) //FIXME: add handing multiple values for intervals
          }
        }

        case _ => None
      }

    } )
    val prunedPaths = if (samples.isEmpty) {
      path
    }
    else {
      val parent = path.split('/').dropRight(1)
      samples.map {
        s => s"${parent.mkString("/")}/${s}*.fastq"
      }
        .mkString(",")
    }
    readFASTQFile(sqlContext,prunedPaths,requiredColumns)
  }

  private def getValueFromColumn(colName:String, r:org.seqdoop.hadoop_bam.SequencedFragment, sampleId:String): Any = {
    colName match {
      case Columns.SAMPLE               => sampleId
      case Columns.BASEQ                => r.getQuality.toString
      case Columns.SEQUENCE             => r.getSequence.toString
      case Columns.INSTRUMENT_NAME      => r.getInstrument
      case Columns.RUN_ID               => r.getRunNumber
      case Columns.FLOWCELL_ID          => r.getFlowcellId
      case Columns.FLOWCELL_LANE        => r.getLane
      case Columns.TILE                 => r.getTile
      case Columns.X_POS                => r.getXpos
      case Columns.Y_POS                => r.getYpos
      case Columns.FILTER_PASSED        => r.getFilterPassed
      case Columns.CONTROL_NUMBER       => r.getControlNumber
      case Columns.INDEX_SEQUENCE       => r.getIndexSequence
      case _              =>    throw new Exception(s"Unknown column found: ${colName}")
    }
  }
  private def readFASTQFile(@transient sqlContext: SQLContext,path: String,
                     requiredColumns:Array[String]):RDD[Row] = {
    val spark = sqlContext
      .sparkSession
    lazy val sequenceFragments = spark
      .sparkContext
      .newAPIHadoopFile[Text,org.seqdoop.hadoop_bam.SequencedFragment, FastqInputFormat](path)

    lazy val sequenceFragmentsWithFilename = sequenceFragments.asInstanceOf[NewHadoopRDD[FastqInputFormat, org.seqdoop.hadoop_bam.SequencedFragment]]
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
        if (inputSplit.isInstanceOf[FileVirtualSplit]) {
          val file =inputSplit.asInstanceOf[FileVirtualSplit]
          iterator.map(tup => (file.getPath.getName.split('.')(0), tup._2))
        }
        else{
          val file = inputSplit.asInstanceOf[FileSplit]
          iterator.map(tup => (file.getPath.getName.split('.')(0), tup._2))
        }
      })

    lazy val sampleSequenceFragments = sequenceFragmentsWithFilename
      .mapPartitions(
        p=> {
          p.map {
            case (sampleId, r) =>
              val record = new Array[Any](requiredColumns.length)
              //requiredColumns.
              for (i <- 0 to requiredColumns.length - 1) {
                record(i) = getValueFromColumn(requiredColumns(i), r, sampleId)
              }
              Row.fromSeq(record)
          }
        }
      )
    sampleSequenceFragments
  }


}
