package org.biodatageeks.sequila.datasources.BAM


import htsjdk.samtools.{SAMRecord, ValidationStringency}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.log4j.Logger
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.biodatageeks.sequila.utils.{Columns, DataQualityFuncs, FastSerializer, InternalParams, ScalaFuncs, TableFuncs}
import org.biodatageeks.formats.Alignment
import org.disq_bio.disq.HtsjdkReadsTraversalParameters

import java.util
import scala.reflect.runtime.universe._
import collection.JavaConverters._
import htsjdk.samtools.util.Locatable

case class Interval(contig: String, posStart: Int, posEnd: Int) extends Locatable with Serializable {
  override def getContig: String = contig

  override def getStart: Int = posStart

  override def getEnd: Int = posEnd
}
//TODO refactor rename to AlignmentsFileReaderWriter
trait BDGAlignFileReaderWriter [T <: BDGAlignInputFormat]{

  val confMap = new mutable.HashMap[String,String]()

  def setLocalConf(@transient sqlContext: SQLContext): confMap.type = {

    val predicatePushdown = sqlContext.getConf("spark.biodatageeks.bam.predicatePushdown","false")
    val gklInflate = sqlContext.getConf("spark.biodatageeks.bam.useGKLInflate","false")
    confMap += ("spark.biodatageeks.bam.predicatePushdown" -> predicatePushdown)
    confMap += ("spark.biodatageeks.bam.useGKLInflate" -> gklInflate)

  }

  def setConf(key:String,value:String): confMap.type = confMap += (key -> value)

  private def setHadoopConf(@transient sqlContext: SQLContext): Unit = {
    setLocalConf(sqlContext)
    val spark = sqlContext
      .sparkSession
    if(confMap("spark.biodatageeks.bam.useGKLInflate").toBoolean)
      spark
        .sparkContext
        .hadoopConfiguration
        .set("hadoopbam.bam.inflate","intel_gkl")
    else
      spark
        .sparkContext
        .hadoopConfiguration
        .unset("hadoopbam.bam.inflate")


    spark
      .sparkContext
      .hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.LENIENT.toString)
  }

  def readBAMFile(@transient sqlContext: SQLContext, path: String, refPath: Option[String] = None)(implicit c: ClassTag[T]): RDD[SAMRecord] = {

    val logger =  Logger.getLogger(this.getClass.getCanonicalName)
    setLocalConf(sqlContext)
    setHadoopConf(sqlContext)




    val spark = sqlContext
      .sparkSession

    val validationStringencyOptLenient = ValidationStringency.LENIENT.toString
    val validationStringencyOptSilent = ValidationStringency.SILENT.toString
    val validationStringencyOptStrict = ValidationStringency.STRICT.toString

    val validationStringency =  spark.sqlContext.getConf(InternalParams.BAMValidationStringency) match {
      case `validationStringencyOptLenient`  => ValidationStringency.LENIENT
      case `validationStringencyOptSilent`   => ValidationStringency.SILENT
      case `validationStringencyOptStrict`   => ValidationStringency.STRICT
      case _ => throw new Exception(s"Unknown validation stringency ${spark.sqlContext.getConf(InternalParams.BAMValidationStringency)}")
    }
    val resolvedPath = TableFuncs.getExactSamplePath(spark,path)
//    val folderPath = TableFuncs.getParentFolderPath(spark,path)
    logger.info(s"######## Reading ${resolvedPath} or ${path}")
    val alignReadMethod = spark.sqlContext.getConf(InternalParams.IOReadAlignmentMethod,"hadoopBAM").toLowerCase
    logger.info(s"######## Using ${alignReadMethod} for reading alignment files.")
    logger.info(s"######## Using inputformat: ${c.toString()}")

    alignReadMethod match {
      case "hadoopbam" => {
        logger.info(s"Using Intel GKL inflater: ${spark.sqlContext.getConf(InternalParams.UseIntelGKL, "false")}")
        val intervals = spark.sqlContext.getConf(InternalParams.AlignmentIntervals,"")
        if (intervals.length > 0){
          logger.info(s"Doing interval queries for intervals ${intervals}")
          spark
            .sparkContext
            .hadoopConfiguration
            .set("hadoopbam.bam.intervals", intervals)
        }
        else
          spark
            .sparkContext
            .hadoopConfiguration
            .unset("hadoopbam.bam.intervals")

        logger.info(s"Setting BAM validation stringency to ${validationStringency.toString}")
        spark
          .sparkContext
          .hadoopConfiguration
          .set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, validationStringency.toString)

        spark.sparkContext
          .newAPIHadoopFile[LongWritable, SAMRecordWritable, T](path)
          .map(r => r._2.get())
      }
      case "disq" => {
        import org.disq_bio.disq.HtsjdkReadsRddStorage
        refPath match {
          case Some(ref) => {
          HtsjdkReadsRddStorage
            .makeDefault(sqlContext.sparkContext)
            .validationStringency(validationStringency)
            .referenceSourcePath(ref)
            .read(resolvedPath)
            .getReads
            .rdd
          }
          case None => {
            val intervals = spark.sqlContext.getConf(InternalParams.AlignmentIntervals,"")
            @transient lazy val ranges = if (intervals.length > 0) {
              logger.info(s"Doing interval queries for intervals ${intervals}")
              intervals
                .split(",")
                .map(_.split("[:,-]"))
                .map(r => new Interval(r(0), r(1).toInt, r(2).toInt))
                .toList
            } else List.empty[Interval]

            val traverseParam = if(ranges.nonEmpty) new HtsjdkReadsTraversalParameters(ranges.asJava, false) else null
            logger.info(s"disq|TraversalParameters: ${if(traverseParam == null) "N/A" else traverseParam.getIntervalsForTraversal.asScala.mkString("|")}")
            HtsjdkReadsRddStorage
              .makeDefault(sqlContext.sparkContext)
              .validationStringency(validationStringency)
              .read(resolvedPath, traverseParam)
              .getReads
              .rdd
          }
          }
        }
    }

  }



  def readBAMFileToBAMBDGRecord(@transient sqlContext: SQLContext, path: String,
                                requiredColumns:Array[String])
                               (implicit c: ClassTag[T]): RDD[Row] = {


    setLocalConf(sqlContext)
    setHadoopConf(sqlContext)
    val ctasCmd = sqlContext.getConf(InternalParams.BAMCTASCmd,"false")
      .toLowerCase
      .toBoolean

    val spark = sqlContext
      .sparkSession
    lazy val alignments = spark
      .sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, T](path)
    lazy val alignmentsWithFileName = alignments.asInstanceOf[NewHadoopRDD[LongWritable, SAMRecordWritable]]
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
    lazy val sampleAlignments = alignmentsWithFileName
      .map(r => (r._1, r._2.get()))
      .mapPartitions(
      p=> {
        val serializer = new FastSerializer
        p.map {
          case (sampleId, r) =>
            val record = new Array[Any](requiredColumns.length)
            //requiredColumns.
            for (i <- requiredColumns.indices) {
              record(i) = getValueFromColumn(requiredColumns(i), r, sampleId,serializer, ctasCmd)
            }
            Row.fromSeq(record)
        }
      }
    )
    sampleAlignments

  }


  private def getValueFromColumn(colName:String, r:SAMRecord, sampleId:String, serializer: FastSerializer, ctasCmd : Boolean): Any = {


    colName match {
      case Columns.SAMPLE =>    sampleId
      case Columns.CONTIG =>    DataQualityFuncs.cleanContig(r.getContig)
      case Columns.START  =>    r.getStart
      case Columns.END    =>    r.getEnd
      case Columns.CIGAR  =>    r.getCigar.toString
      case Columns.MAPQ   =>    r.getMappingQuality
      case Columns.BASEQ  =>    r.getBaseQualityString
      case Columns.SEQUENCE =>  r.getReadString
      case Columns.FLAGS  =>    r.getFlags
      case Columns.QNAME  =>    r.getReadName
      case Columns.POS    =>    r.getStart
      case Columns.RNEXT  =>    DataQualityFuncs.cleanContig(r.getMateReferenceName) //FIXME: to check if the mapping is correct
      case Columns.PNEXT  =>    r.getMateAlignmentStart //FIXME: to check if the mapping is correct
      case Columns.TLEN   =>    r.getLengthOnReference //FIXME: to check if the mapping is correct
      case s if s.startsWith("tag_")    => {
        val fields = ScalaFuncs.classAccessors[Alignment]
        val tagField = fields.filter(_.name.toString == s).head
        val tagFieldName = tagField.name.toString.replaceFirst("tag_", "").toUpperCase
        val tagFieldType = tagField.typeSignature.resultType
        tagFieldType match {
          case t if t =:= typeOf[Option[Long]]  => {val v = r.getUnsignedIntegerAttribute(tagFieldName); v }
          case t if t =:= typeOf[Option[Int]]  => {val v = r.getIntegerAttribute(tagFieldName); v }
          case t if t =:= typeOf[Option[String]] => {val v = r.getStringAttribute(tagFieldName); v}
          case _ => throw new Exception (s"Cannot process ${tagFieldName} with type: ${tagFieldType}.")
        }
      }
      case _              =>    throw new Exception(s"Unknown column found: ${colName}")
    }

  }


}

class BDGAlignmentRelation[T <:BDGAlignInputFormat](path:String, refPath:Option[String] = None)(@transient val sqlContext: SQLContext)(implicit c: ClassTag[T])
  extends BaseRelation
    with PrunedFilteredScan
    with Serializable
    with BDGAlignFileReaderWriter[T] {

  @transient val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)
  val spark: SparkSession = sqlContext
    .sparkSession
  setLocalConf(sqlContext)

  val tablePath: String = path

  spark
    .sparkContext
    .hadoopConfiguration
    .set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, InternalParams.BAMValidationStringency)

  //CRAM reference file
  refPath match {
    case Some(p) => {
      sqlContext
        .sparkContext
        .hadoopConfiguration
        .set(CRAMBDGInputFormat.REFERENCE_SOURCE_PATH_PROPERTY,p)
    }
    case _ => None
  }

  override def schema: org.apache.spark.sql.types.StructType = Encoders.product[Alignment].schema


  override def buildScan(requiredColumns:Array[String], filters:Array[Filter]): RDD[Row] = {

    val logger = Logger.getLogger(this.getClass.getCanonicalName)

    val samples = ArrayBuffer[String]()
      logger.info(s"Got filter: ${filters.mkString("|")}")
      val gRanges = ArrayBuffer[String]()
      var contigName: String = ""
      var startPos = 0
      var endPos = 0
      var pos = 0

      filters.foreach(f => {
        f match {
          case EqualTo(attr, value) => {
            if (attr.equalsIgnoreCase(Columns.SAMPLE))
              samples += value.toString
          }
            if (attr.equalsIgnoreCase(Columns.CONTIG) ) contigName = value.toString
            if (attr.equalsIgnoreCase(Columns.START) || attr.equalsIgnoreCase(Columns.END) ) { //handle predicate contigName='chr1' AND start=2345
              pos = value.asInstanceOf[Int]
            }
          case In(attr, values) => {
            if (attr.equalsIgnoreCase(Columns.SAMPLE) ) {
              values.foreach(s => samples += s.toString) //FIXME: add handing multiple values for intervals
            }
          }

          case LessThanOrEqual(attr, value) => {
            if (attr.equalsIgnoreCase(Columns.START) || attr.equalsIgnoreCase(Columns.END) ) {
              endPos = value.asInstanceOf[Int]
            }
          }

          case LessThan(attr, value) => {
            if (attr.equalsIgnoreCase(Columns.START) ||  attr.equalsIgnoreCase(Columns.END) ) {
              endPos = value.asInstanceOf[Int]
            }
          }

          case GreaterThanOrEqual(attr, value) => {
            if (attr.equalsIgnoreCase(Columns.START) ||  attr.equalsIgnoreCase(Columns.END) ) {
              startPos = value.asInstanceOf[Int]
            }

          }
          case GreaterThan(attr, value) => {
            if (attr.equalsIgnoreCase(Columns.START) ||  attr.equalsIgnoreCase(Columns.END) ) {
              startPos = value.asInstanceOf[Int]
            }
          }


          case _ => None
        }

        if (contigName != "") {
          if (pos > 0) {
            gRanges += s"${contigName}:${pos.toString}-${pos.toString}"
            pos = 0
            contigName = ""
          }
          else if (startPos > 0 && endPos > 0) {
            gRanges += s"${contigName}:${startPos.toString}-${endPos.toString}"
            startPos = 0
            endPos = 0
            contigName = ""

          }
        }
      }

      )
      val prunedPaths = if (samples.isEmpty) {
        path
      }
      else {
        val parent = path.split('/').dropRight(1)
        samples.map {
          s => s"${parent.mkString("/")}/${s}*.bam"
        }
          .mkString(",")
      }
      if (prunedPaths != path) logger.info(s"Partition pruning detected, reading only files for samples: ${samples.mkString(",")}")

    //FIXME: Currently only rname with chr prefix is assumed is added in the runtime, may cause issues in BAMs with differnt prefix!
      logger.info(s"GRanges: ${gRanges.mkString(",")}, ${spark.sqlContext.getConf("spark.biodatageeks.bam.predicatePushdown", "false")}")
      if (gRanges.length > 0 && spark.sqlContext.getConf("spark.biodatageeks.bam.predicatePushdown", "false").toBoolean) {
        logger.info(s"Interval query detected and predicate pushdown enabled, trying to do predicate pushdown using intervals ${gRanges.mkString("|")}")
        setConf("spark.biodatageeks.bam.intervals", gRanges.mkString(","))
      }
      else
        setConf("spark.biodatageeks.bam.intervals", "")

      readBAMFileToBAMBDGRecord(sqlContext, prunedPaths, requiredColumns)
    }

  //optimized scan for queries like SELECT (distinct )sampleId FROM  BDGAlignmentRelation do not touch files, only file names
  def buildScanSampleId = {
    spark
      .sparkContext
      .parallelize(TableFuncs.getAllSamples(spark,path))
      .map(r=>Row.fromSeq(Seq(r)) )
  }

  def buildScanWithLimit(requiredColumns:Array[String], filters:Array[Filter], limit: Int ): RDD[Row] = {

    logger.info(s"################Using optimized SCAN for LIMIT clause")
    spark
      .sparkContext
      .parallelize(buildScan(requiredColumns, filters).take(limit) )
  }


}