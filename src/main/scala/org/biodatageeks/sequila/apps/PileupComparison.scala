package org.biodatageeks.sequila.apps

import java.io.File

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.{DataFrame, Dataset, Row, SequilaSession}
import org.biodatageeks.sequila.pileup.PileupWriter
import org.biodatageeks.sequila.pileup.converters.common.PileupFormats.{SAMTOOLS_FORMAT, SAMTOOLS_FORMAT_SHORT, GATK_FORMAT, SEQUILA_FORMAT}
import org.biodatageeks.sequila.pileup.converters.gatk.GatkConverter
import org.biodatageeks.sequila.pileup.converters.samtools.SamtoolsConverter
import org.biodatageeks.sequila.pileup.converters.sequila.SequilaConverter

import scala.collection.mutable


object PileupComparison extends App with SequilaApp with DatasetComparer {

  override def main(args: Array[String]): Unit = {
    checkArgs(args)
    val ss = createSequilaSession()
    val files = combineFileWithFomat(args)

    val dfByFormat = files.map(file =>(file._2, convert(ss, file._1, file._2.toLowerCase(), save = true) ))
    val res = crossCompare(dfByFormat)
    printResults(res)
  }

  def crossCompare (pileupList: Array [(String, Dataset[Row])]): Map[(String, String), (Int,String)] = {
    val result = new mutable.HashMap[(String, String), (Int, String)]
    val dfByFormatCombinations = pileupList.combinations(2)
    for (pair <- dfByFormatCombinations) {
      try {
        assertLargeDatasetEquality(pair(0)._2, pair(1)._2)
        result += (pair(0)._1, pair(1)._1) -> (0, "EQUAL")
      } catch {
        case e: Exception => result += (pair(0)._1, pair(1)._1) ->  (1, s"NOT EQUAL ${e.getLocalizedMessage}")
      }
    }
    result.toMap
  }

  private def combineFileWithFomat(args: Array[String]): Array[(String, String)] = {
    args.grouped(2).toArray.map{case Array(a,b) => (a,b)}
  }

  def convert(ss:SequilaSession, file: String, format:String, save:Boolean=false): Dataset[Row] = {
    val df = format match {
      case SAMTOOLS_FORMAT | SAMTOOLS_FORMAT_SHORT  => new SamtoolsConverter(ss).transform(file)
      case SEQUILA_FORMAT => new SequilaConverter(ss).transform(file)
      case GATK_FORMAT => new GatkConverter(ss).transform(file)
      case _ => throw new RuntimeException("Unknown file format")
    }
    if (save) {
      val outName = prepareOutFilename(file, format)
      PileupWriter.save(df,outName)
    }
    printDf(df, format)
    df
  }

  private def prepareOutFilename(file: String, format: String):String = {
    s"${new File(file).getParent}/${format}_out.csv"
  }

  private def checkArgs(args: Array[String]): Unit = {
    val minArgs = 4
    if (args.isEmpty)
      throw new RuntimeException("Please supply input arguments")

    if (args.length < minArgs)
      throw new RuntimeException("Provide at least two  files for comparison")

    if ((args.length%2) != 0)
      throw new RuntimeException("Each file needs format specification (sam, or sequila or gatk)")
  }

  private def printResults (res: Map[(String, String), (Int,String)]): Unit = {
    res.foreach(x =>
      println ((if (x._2._1 != 0) Console.RED else Console.GREEN) + s">> ${x._1._1.toUpperCase()} with ${x._1._2.toUpperCase()}: result: ${x._2._2}"))
  }

  private def printDf(df: DataFrame, format: String): Unit = {
    println(s"$format: ${df.count} ${df.rdd.getNumPartitions}")
    df.printSchema()
    df.show(10)
  }
}
