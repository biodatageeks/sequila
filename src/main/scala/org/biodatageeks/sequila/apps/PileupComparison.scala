package org.biodatageeks.sequila.apps

import org.apache.spark.sql.{DataFrame, Dataset, Row, SequilaSession, SparkSession}
import org.biodatageeks.sequila.pileup.converters.{CommonPileupFormat, SamtoolsConverter, SamtoolsSchema}
import org.biodatageeks.sequila.utils.{Columns, SequilaRegister}


object PileupComparison extends App{
  override def main(args: Array[String]): Unit = {
    checkArgs(args)

    val files = combineFileWithFomat(args)
    val ss = createSparkSession()

    val commonFormat = files.map(file =>(file._2, convert(ss, file._1, file._2.toLowerCase())))

  }

  private def combineFileWithFomat(args: Array[String]): Array[(String, String)] = {
    args.zip(args.tail)
  }

  def convertSamtoolsFile(ss: SequilaSession, file: String): DataFrame = {
    val df = ss.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(SamtoolsSchema.schema)
      .load(file)

    val converter = new SamtoolsConverter(ss)
    val sam = converter
      .transformSamToBlocks(df, caseSensitive = true)
      .select(Columns.CONTIG, Columns.START, Columns.END,Columns.REF,  Columns.COVERAGE, Columns.ALTS)
      .orderBy("contig", "pos_start")
    ss.createDataFrame(sam.rdd, CommonPileupFormat.schema)
  }

  def convertSequilaFile(ss: SequilaSession, file: String): DataFrame = {
    val df = ss.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(CommonPileupFormat.schema)
      .load(file)
    df
  }

  def convert(ss:SequilaSession, file: String, format:String): Dataset[Row] = {
    format match {
      case "sam" | "samtools" => {
        println ("Converting samtools result " + file)
        convertSamtoolsFile(ss, file)
      }
      case "sequila" => {
        println ("seq " + file)
        convertSequilaFile(ss,file)
        ss.emptyDataFrame
      }
      case "gatk" => throw new NoSuchMethodException  ("GATK is not supported yet ")
    }
  }
  def createSparkSession (): SequilaSession = {
    System.setProperty("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.driver.memory","4g")
      .config( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .getOrCreate()

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    spark.sparkContext.setLogLevel("INFO")
    ss
  }

  private def checkArgs (args: Array[String]): Unit = {
    val minArgs = 4
    if (args.isEmpty)
      throw new RuntimeException("Please supply input arguments")

    if (args.length < minArgs)
      throw new RuntimeException("Provide at least two  files for comparison")

    if ((args.length%2) != 0)
      throw new RuntimeException("Each file needs format specification (sam, or sequila or gatk)")

  }
}
