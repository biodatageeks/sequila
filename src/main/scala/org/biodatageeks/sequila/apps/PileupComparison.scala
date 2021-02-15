package org.biodatageeks.sequila.apps

import org.apache.spark.sql.{DataFrame, Dataset, Row, SequilaSession}
import org.biodatageeks.sequila.pileup.converters.{CommonPileupFormat, SamtoolsConverter, SamtoolsSchema}
import org.biodatageeks.sequila.utils.Columns


object PileupComparison extends App with SequilaApp {

  override def main(args: Array[String]): Unit = {
    checkArgs(args)

    val files = combineFileWithFomat(args)
    val ss = createSequilaSession()

    val dfByFormat = files.map(file =>(file._2, convert(ss, file._1, file._2.toLowerCase())))

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
      .orderBy("contig", "pos_start")

    val convertedSam = mapColumnsAsStrings(sam)
    println("SAMTOOLS")
    convertedSam.printSchema()
    convertedSam.show(10)
    convertedSam
  }

  private def mapColumnsAsStrings(df: DataFrame): DataFrame = {
    val indA = df.columns.indexOf({Columns.ALTS})
    val indQ = df.columns.indexOf({Columns.QUALS})

    val outputColumns =  df.columns
    outputColumns(indA) = s"CAST (alts_to_char(${Columns.ALTS}) as String) as ${Columns.ALTS}"
    outputColumns(indQ) = s"CAST (quals_to_char(${Columns.QUALS}) as String) as ${Columns.QUALS}"

    val convertedDf = df.selectExpr(outputColumns: _*)
    convertedDf

  }

  def convertSequilaFile(ss: SequilaSession, file: String): DataFrame = {
    val df = ss.read
      .format("csv")
      .schema(CommonPileupFormat.fileSchema)
      .load(file)

    val convertedDf = mapColumnsAsStrings(df)
    println ("SEQUILA FORMAT:")
    convertedDf.printSchema()
    convertedDf.show(10)
    convertedDf
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