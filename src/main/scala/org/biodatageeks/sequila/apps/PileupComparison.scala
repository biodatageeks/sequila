package org.biodatageeks.sequila.apps

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.{DataFrame, Dataset, Row, SequilaSession}
import org.biodatageeks.sequila.pileup.converters.gatk.{GatkConverter, GatkSchema}
import org.biodatageeks.sequila.pileup.converters.samtools.{SamtoolsConverter, SamtoolsSchema}
import org.biodatageeks.sequila.pileup.converters.CommonPileupFormat
import org.biodatageeks.sequila.pileup.converters.sequila.SequilaConverter
import org.biodatageeks.sequila.utils.Columns


object PileupComparison extends App with SequilaApp with DatasetComparer {

  override def main(args: Array[String]): Unit = {
    checkArgs(args)
    val ss = createSequilaSession()
    val files = combineFileWithFomat(args)

    val dfByFormat = files.map(file =>(file._2, convert(ss, file._1, file._2.toLowerCase())))
    val dfByFormatCombinations = dfByFormat.combinations(2)
    for (pair <- dfByFormatCombinations) {
      try {
        assertLargeDatasetEquality(pair(0)._2, pair(1)._2)
        println(s"${pair(0)._1} equal to ${pair(1)._1}")
      } catch {
        case e: Exception => println(s"${pair(0)._1} not equal to ${pair(1)._1} \n ${e.getLocalizedMessage}")
      }
    }
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
      .transformToCommonFormat(df, caseSensitive = true)
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
    outputColumns(indA) = s"altmap_to_str(alts_to_char(${Columns.ALTS})) as ${Columns.ALTS}"
    outputColumns(indQ) = s"qualsmap_to_str(quals_to_char(${Columns.QUALS})) as ${Columns.QUALS}"

    val convertedDf = df.selectExpr(outputColumns: _*)
    convertedDf

  }

  def convertSequilaFile(ss: SequilaSession, file: String): DataFrame = {
    val df = ss.read
      .format("csv")
      .schema(CommonPileupFormat.fileSchema)
      .load(file)

    val sequilaConverter = new SequilaConverter(ss)
    val converted = sequilaConverter.transformToCommonFormat(df, true)

    println ("SEQUILA FORMAT:")
    converted.printSchema()
    converted.show(10)
    converted
  }

  def convertGatkFile(ss: SequilaSession, file: String): DataFrame = {
    val df = ss.read
      .format("csv")
      .option("delimiter", " ")
      .schema(GatkSchema.schema)
      .load(file)

    val converter = new GatkConverter(ss)
    val convertedGatk = converter
      .transformToCommonFormat(df, caseSensitive = true)
      .orderBy("contig", "pos_start")


    val finalGatk = mapColumnsAsStrings(convertedGatk)

    println ("GATK FORMAT:")
    finalGatk.printSchema()
    finalGatk.show(10)
    finalGatk
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
      case "gatk" => {
        println (s"gatk format on file $file")
        convertGatkFile(ss, file)
      }
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
