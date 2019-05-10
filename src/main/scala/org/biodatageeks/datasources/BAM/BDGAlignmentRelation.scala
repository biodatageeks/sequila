package org.biodatageeks.datasources.BAM

import java.net.URI

import htsjdk.samtools.{SAMRecord, ValidationStringency}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem}
import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.biodatageeks.inputformats.BDGAlignInputFormat
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.spark.rdd.PairRDDFunctions
import org.biodatageeks.outputformats.BAMBDGOutputFormat
import org.biodatageeks.utils.{BDGFastSerializer, BDGInternalParams, BDGTableFuncs}
import org.nustaq.serialization.FSTConfiguration


case class BDGSAMRecord(sampleId: String,
                     contigName:String,
                     start:Int,
                     end:Int,
                     cigar:String,
                     mapq:Int,
                     baseq: String,
                     sequence:String,
                     flags:Int,
                     materefind:Int,
                     SAMRecord: Option[Array[Byte]])


trait BDGAlignFileReaderWriter [T <: BDGAlignInputFormat]{



//  val bdgSerialize = new BDGSerializer()
  //val serializer = new BDGFastSerializer()
  val confMap = new mutable.HashMap[String,String]()
  val columnNames = Array(
    "sampleId",
    "contigName",
    "start",
    "end",
    "cigar",
    "mapq",
    "baseq",
    "sequence",
    "flags",
    "materefind",
    "SAMRecord"
  )

  def setLocalConf(@transient sqlContext: SQLContext) = {

    val predicatePushdown = sqlContext.getConf("spark.biodatageeks.bam.predicatePushdown","false")
    val gklInflate = sqlContext.getConf("spark.biodatageeks.bam.useGKLInflate","false")
    confMap += ("spark.biodatageeks.bam.predicatePushdown" -> predicatePushdown)
    confMap += ("spark.biodatageeks.bam.useGKLInflate" -> gklInflate)

  }

  def setConf(key:String,value:String) = confMap += (key -> value)

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

    confMap.get("spark.biodatageeks.bam.intervals") match {
      case Some(s) => {
        if(s.length > 0)
        spark
          .sparkContext
          .hadoopConfiguration
          .set("hadoopbam.bam.intervals", s)
        else
          spark
            .sparkContext
            .hadoopConfiguration
            .unset("hadoopbam.bam.intervals")

      }
        case _ => None
      }

    spark
      .sparkContext
      .hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.LENIENT.toString)
  }

  def readBAMFile(@transient sqlContext: SQLContext, path: String, refPath: Option[String] = None)(implicit c: ClassTag[T]) = {

    val logger =  Logger.getLogger(this.getClass.getCanonicalName)
    setLocalConf(sqlContext)
    setConf("spark.biodatageeks.bam.intervals","") //FIXME: disabled PP
    setHadoopConf(sqlContext)




    val spark = sqlContext
      .sparkSession
    val resolvedPath = BDGTableFuncs.getExactSamplePath(spark,path)
//    val folderPath = BDGTableFuncs.getParentFolderPath(spark,path)
    logger.info(s"######## Reading ${resolvedPath} or ${path}")
    val alignReadMethod = spark.sqlContext.getConf(BDGInternalParams.IOReadAlignmentMethod,"hadoopBAM").toLowerCase
    logger.info(s"######## Using ${alignReadMethod} for reading alignment files.")
    logger.info(s"######## Using inputformat: ${c.toString()}")

    alignReadMethod match {
      case "hadoopbam" => {
        logger.info(s"Using Intel GKL inflater: ${BDGInternalParams.UseIntelGKL}")
        spark.sparkContext
          .newAPIHadoopFile[LongWritable, SAMRecordWritable, T](path)
          .map(r => r._2.get())
      }
      case "sparkbam" => {
        import spark_bam._, hammerlab.path._
        val bamPath = Path(resolvedPath)
        spark
          .sparkContext
          .loadReads(bamPath)
      }

      case "disq" => {
        import org.disq_bio.disq.HtsjdkReadsRddStorage

        refPath match {
          case Some(ref) => {
          HtsjdkReadsRddStorage
            .makeDefault(sqlContext.sparkContext)
            .validationStringency(ValidationStringency.LENIENT)
            .referenceSourcePath(ref)
            .read(resolvedPath)
            .getReads
            .rdd
          }
          case None => {
            HtsjdkReadsRddStorage
              .makeDefault(sqlContext.sparkContext)
              .validationStringency(ValidationStringency.LENIENT)
              .read(resolvedPath)
              .getReads
              .rdd
          }
          }
        }
    }

  }



  def readBAMFileToBAMBDGRecord(@transient sqlContext: SQLContext, path: String,
                                requiredColumns:Array[String])
                               (implicit c: ClassTag[T]) = {


    setLocalConf(sqlContext)
    setHadoopConf(sqlContext)
    val ctasCmd = sqlContext.getConf(BDGInternalParams.BAMCTASCmd,"false")
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
        val serializer = new BDGFastSerializer
        p.map {
          case (sampleId, r) =>
            val record = new Array[Any](requiredColumns.length)
            //requiredColumns.
            for (i <- 0 to requiredColumns.length - 1) {
              record(i) = getValueFromColumn(requiredColumns(i), r, sampleId,serializer, ctasCmd)
            }
            Row.fromSeq(record)
        }
      }
    )
    sampleAlignments

  }

  def saveAsBAMFile(sqlContext: SQLContext, rdd:RDD[SAMRecord], path:String, headerPath:String) = {


//    val header = rdd.first().getHeader
//    val bdgSerializer = new BDGSerializer(header)
    val nullPathString = "/tmp/null.bam"
    //Fix for Spark saveAsNewHadoopfile
    val hdfs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    val nullPath = new org.apache.hadoop.fs.Path(nullPathString)
    if(hdfs.exists(nullPath)) hdfs.delete(nullPath,true)


    sqlContext
      .sparkContext
      .hadoopConfiguration
      .set(BDGInternalParams.BAMCTASHeaderPath,headerPath)

    sqlContext
      .sparkContext
      .hadoopConfiguration
      .set(BDGInternalParams.BAMCTASOutputPath,path)

    try {
      rdd
        .map(r => (NullWritable.get(),  {val record = new SAMRecordWritable();record.set(r);record}) )
        .saveAsNewAPIHadoopFile[BAMBDGOutputFormat[NullWritable]](nullPathString)
    }
    finally {
      sqlContext
        .sparkContext
        .hadoopConfiguration
        .unset(BDGInternalParams.BAMCTASHeaderPath)
      sqlContext
        .sparkContext
        .hadoopConfiguration
        .unset(BDGInternalParams.BAMCTASOutputPath)
      sqlContext.setConf(BDGInternalParams.BAMCTASCmd,"false")
    }


  }

  private def getValueFromColumn(colName:String,r:SAMRecord, sampleId:String, serializer: BDGFastSerializer,ctasCmd : Boolean): Any = {




    if(colName == columnNames(0)) sampleId
    else if (colName == columnNames(1)) r.getContig
    else if (colName == columnNames(2)) r.getStart
    else if (colName == columnNames(3)) r.getEnd
    else if (colName == columnNames(4)) r.getCigar.toString
    else if (colName == columnNames(5)) r.getMappingQuality
    else if (colName == columnNames(6)) r.getBaseQualityString
    else if (colName == columnNames(7)) r.getReadString
    else if (colName == columnNames(8)) r.getFlags
    else if (colName == columnNames(9)) r.getMateReferenceIndex
    else if (colName == columnNames(10)) if(ctasCmd)
        serializer.fst.asByteArray(r)
      else None
    else throw new Exception("Unknown column")

  }


}

class BDGAlignmentRelation[T <:BDGAlignInputFormat](path:String, refPath:Option[String] = None)(@transient val sqlContext: SQLContext)(implicit c: ClassTag[T])
  extends BaseRelation
    with InsertableRelation
    with PrunedFilteredScan
    //with CatalystScan
    with Serializable
    with BDGAlignFileReaderWriter[T] {


  val spark = sqlContext
    .sparkSession
  setLocalConf(sqlContext)

  val tablePath = path

  spark
    .sparkContext
    .hadoopConfiguration
    .set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)

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
        new StructField(columnNames(9), IntegerType),
        new StructField(columnNames(10), BinaryType)
      )
    )
  }

  override def buildScan(requiredColumns:Array[String], filters:Array[Filter]): RDD[Row] = {

    val logger = Logger.getLogger(this.getClass.getCanonicalName)

    //optimization if only sampleId column is referenced, does not work for count(*) so rolling back
/*  if(requiredColumns.length == 1 && (requiredColumns.head.toLowerCase == "sampleid"
    || requiredColumns.head.toLowerCase == "sample_id")){

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val statuses = fs.globStatus(new org.apache.hadoop.fs.Path(path))
    logger.warn("Only sampleId column is referenced, skipping BAM files reading.")
    statuses.foreach(r=>println(r.getPath.toString))
    spark
      .sparkContext
      .parallelize(
      statuses
        .map(r=>
          Row.fromSeq(Seq(r.getPath
          .toString
          .split('/')
          .takeRight(1)(0)
            .split('.')(0)) )
    )
   )
  }*/
    //else {
      val samples = ArrayBuffer[String]()

      val gRanges = ArrayBuffer[String]()
      var contigName: String = ""
      var startPos = 0
      var endPos = 0
      var pos = 0

      filters.foreach(f => {
        f match {
          case EqualTo(attr, value) => {
            if (attr.toLowerCase == "sampleid" || attr.toLowerCase == "sample_id")
              samples += value.toString
          }
            if (attr.toLowerCase == "contigname") contigName = value.toString
            if (attr.toLowerCase == "start" || attr.toLowerCase() == "end") { //handle predicate contigName='chr1' AND start=2345
              pos = value.asInstanceOf[Int]
            }
          case In(attr, values) => {
            if (attr.toLowerCase == "sampleid" || attr.toLowerCase == "sample_id") {
              values.foreach(s => samples += s.toString) //FIXME: add handing multiple values for intervals
            }
          }

          case LessThanOrEqual(attr, value) => {
            if (attr.toLowerCase == "start" || attr.toLowerCase == "end") {
              endPos = value.asInstanceOf[Int]
            }
          }

          case LessThan(attr, value) => {
            if (attr.toLowerCase == "start" || attr.toLowerCase == "end") {
              endPos = value.asInstanceOf[Int]
            }
          }

          case GreaterThanOrEqual(attr, value) => {
            if (attr.toLowerCase == "start" || attr.toLowerCase == "end") {
              startPos = value.asInstanceOf[Int]
            }

          }
          case GreaterThan(attr, value) => {
            if (attr.toLowerCase == "start" || attr.toLowerCase == "end") {
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
      if (prunedPaths != path) logger.warn(s"Partition pruning detected, reading only files for samples: ${samples.mkString(",")}")

      logger.warn(s"GRanges: ${gRanges.mkString(",")}, ${spark.sqlContext.getConf("spark.biodatageeks.bam.predicatePushdown", "false")}")
      if (gRanges.length > 0 && spark.sqlContext.getConf("spark.biodatageeks.bam.predicatePushdown", "false").toBoolean) {
        logger.warn(s"Interval query detected and predicate pushdown enabled, trying to do predicate pushdown using intervals ${gRanges.mkString("|")}")
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
      .parallelize(BDGTableFuncs.getAllSamples(spark,path))
      .map(r=>Row.fromSeq(Seq(r)) )
  }

  override  def insert(data: DataFrame,overwrite:Boolean) = ???

  def insertWithHeader(data:DataFrame, overwrite:Boolean, srcTable:String) = {

    sqlContext.setConf(BDGInternalParams.BAMCTASCmd,"true")
    import spark.implicits._

    val ds = data
      .as[BDGSAMRecord]
    val sampleName = ds.first().sampleId
    val outPathString = s"${path.split('/').dropRight(1).mkString("/")}/${sampleName}.bam"

    val outPath = new org.apache.hadoop.fs.Path(outPathString)
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var fos:FSDataOutputStream  = null

    if (hdfs.exists(outPath) && overwrite) {
      hdfs.delete(outPath,true)
      fos = hdfs.create(outPath)
    }
    else if(!hdfs.exists(outPath)) { //
      fos = hdfs.create(outPath)
    }
    else
      throw new Exception(s"Path: ${outPathString} already exits and saveMode is not ${SaveMode.Overwrite}")

    fos.close()

   lazy val srcBAMRDD =
      ds
        .rdd
        .mapPartitions(p =>{
          val bdgSerializer = new BDGFastSerializer()
          p.map( r => bdgSerializer.fst.asObject(r.SAMRecord.get).asInstanceOf[SAMRecord] )
        })

    val samplePath = s"${BDGTableFuncs.getTableDirectory(spark,srcTable)}/${sampleName}*.bam"
    val headerPath = BDGTableFuncs.getExactSamplePath(spark,samplePath)
    saveAsBAMFile(spark.sqlContext,srcBAMRDD,outPathString,headerPath)
  }

}