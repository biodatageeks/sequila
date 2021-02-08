package org.biodatageeks.sequila.apps

import org.apache.spark.sql.{DataFrame, Dataset, Row, SequilaSession, SparkSession}
import org.biodatageeks.sequila.pileup.converters.{CommonPileupFormat, SamtoolsConverter, SamtoolsSchema}
import org.biodatageeks.sequila.utils.{Columns, SequilaRegister}


object PileupComparison extends App with SequilaApp {
  override def main(args: Array[String]): Unit = {
    checkArgs(args)

    val files = combineFileWithFomat(args)
    val ss = createSequilaSession()

    val commonFormat = files.map(file =>(file._2, convert(ss, file._1, file._2.toLowerCase())))

  }

  private def combineFileWithFomat(args: Array[String]): Array[(String, String)] = {
    args.grouped(2).toArray.map{case Array(a,b) => (a,b)}
  }

  def convertSamtoolsFile(ss: SequilaSession, file: String): DataFrame = {
    val df = ss.read
      .format("csv")
      .option("delimiter", "\t")
      .option("quote", "\u0000")
      .schema(SamtoolsSchema.schema)
      .load(file)

    val converter = new SamtoolsConverter(ss)
    val sam = converter
      .transformSamToBlocks(df, caseSensitive = true)
      .select(Columns.CONTIG, Columns.START, Columns.END,Columns.REF,  Columns.COVERAGE, Columns.ALTS)
      .orderBy("contig", "pos_start")
    val convertedSam = ss.createDataFrame(sam.rdd, CommonPileupFormat.schema)
    convertedSam.printSchema()
    convertedSam.show(10)
    convertedSam
  }

  def convertSequilaFile(ss: SequilaSession, file: String): DataFrame = {
    val df = ss.read
      .format("csv")
      .schema(CommonPileupFormat.fileSchema)
      .load(file)
    df.printSchema()
    df.show(10)
    df
  }

  def convert(ss:SequilaSession, file: String, format:String): Dataset[Row] = {
    format match {
      case "sam" | "samtools" => {
        println (s"Samtools format on file $file")
        convertSamtoolsFile(ss, file)
      }
      case "sequila" => {
        println (s"Sequila format on file $file")
        convertSequilaFile(ss,file)
      }
      case "gatk" => throw new NoSuchMethodException  ("GATK is not supported yet ")
    }
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
