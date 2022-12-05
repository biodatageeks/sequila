package org.biodatageeks.sequila.ximmer.converters

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

class GngsConverter {
  var coveragesByRegions : mutable.Map[RegionWithSize, ListBuffer[Double]] = mutable.LinkedHashMap[RegionWithSize, ListBuffer[Double]]()
  // Zmienna pomagajaca uwzglednic basy z zerowym pokryciem w statystykach
  var targetRegionsSize = 0

  def calculateStatsAndConvertToGngsFormat(targetsPerBaseFile: String, targetsFile: String, outputPath: String, sample: String): Unit = {

    initTargetRegions(targetsFile)
    readCoverages(targetsPerBaseFile)

    val coverages = coveragesByRegions.values
      .toStream
      .flatMap(x => x.toStream)
      .sorted
      .toList

    calculateAndWriteStats(coverages, outputPath, sample)
    writeSampleIntervalSummary(outputPath, sample)
  }

  private def initTargetRegions(targetsFile: String): Unit = {
    val content = Source.fromFile(targetsFile)

    for (line <- content.getLines) {
      val elements = line.split("\t")
      val regionWithSize = convertLineToObject(elements)
      coveragesByRegions += (regionWithSize -> new ListBuffer[Double])
      targetRegionsSize += regionWithSize.size
    }

    content.close()
  }

  private def readCoverages(joinFile: String): Unit = {
    val content = Source.fromFile(joinFile)

    for (line <- content.getLines) {
      val elements = line.split(",")
      val regionWithSize = convertLineToObject(elements)

      var coverage = elements(3)
      if (coverage == "\"\"" || coverage.isEmpty) {
        coverage = "0.0"
      }
      val coverageDouble = coverage.toDouble

      coveragesByRegions(regionWithSize) += coverageDouble
    }

    content.close()
  }

  private def calculateAndWriteStats(coverages: List[Double], outputPath: String, sample: String) : Unit = {
    val median = calculateMedian(coverages)
    val mean = coverages.sum / targetRegionsSize
    val perc_bases_above_1 = calcAbovePercent(coverages, 1)
    val perc_bases_above_5 = calcAbovePercent(coverages, 5)
    val perc_bases_above_10 = calcAbovePercent(coverages, 10)
    val perc_bases_above_20 = calcAbovePercent(coverages, 20)
    val perc_bases_above_50 = calcAbovePercent(coverages, 50)

    val fileObject = new File(outputPath + "/" +sample + ".stats.tsv" )
    val pw = new PrintWriter(fileObject)
    pw.write("Median Coverage\tMean Coverage\tperc_bases_above_1\tperc_bases_above_5\tperc_bases_above_10\t" +
      "perc_bases_above_20\tperc_bases_above_50")
    pw.write("\n")
    pw.write(median + "\t" + mean + "\t" + perc_bases_above_1 + "\t" + perc_bases_above_5 + "\t" + perc_bases_above_10
      + "\t" + perc_bases_above_20 + "\t" + perc_bases_above_50 + "\t")
    println(s"Write file " + outputPath + "/sample_interval_summary/" + sample + ".stats.tsv")
    pw.close()
  }

  private def calculateMedian(sortedValues: List[Double]) : Double = {
    val nrOfZeroCoverages = targetRegionsSize - sortedValues.size
    val allValues = Array.fill(nrOfZeroCoverages)(0.0) ++ sortedValues

    if (allValues.length % 2 == 1) {
      allValues(allValues.length / 2)
    } else {
      val (up, down) = allValues.splitAt(allValues.length / 2)
      (up.last + down.head) / 2
    }
  }

  private def calcAbovePercent(values: List[Double], above : Int) : Double = {
    val nrAbove = values.toStream
      .count(x => x >= above)
    nrAbove.toDouble / targetRegionsSize * 100
  }

  private def writeSampleIntervalSummary(outputPath: String, sample: String): Unit = {
    val regions = new ListBuffer[String]()
    val means = new ListBuffer[String]()
    regions += "sample"
    means += sample

    coveragesByRegions.foreach(x => {
      val region = x._1.region
      val regionSize = x._1.size
      val sum = x._2.sum
      regions += region
      means += (sum / regionSize).toString
    })

    val fileObject = new File(outputPath + "/" + sample + ".calc_target_covs.sample_interval_summary" )
    val pw = new PrintWriter(fileObject)
    pw.write(regions.toList.mkString("\t"))
    pw.write("\n")
    pw.write(means.toList.mkString("\t"))
    println(s"Write file " + outputPath + "/sample_interval_summary/" + sample + ".calc_target_covs.sample_interval_summary")
    pw.close()
  }

  private def convertLineToObject(elements: Array[String]) : RegionWithSize = {
    val region = elements(0) + ":" + elements(1) + "-" + elements(2)
    val size = elements(2).toInt - elements(1).toInt + 1
    new RegionWithSize(region, size)
  }

  class RegionWithSize(var region: String, var size: Int) {
    override def equals(o: Any): Boolean = {
      if (!o.isInstanceOf[RegionWithSize]) return false
      val other = o.asInstanceOf[RegionWithSize]

      (this.region == other.region) && (this.size == other.size)
    }

    override def hashCode(): Int = {
      val PRIME = 59
      region.hashCode() * PRIME + size.hashCode()
    }
  }

}
