package org.biodatageeks.sequila.ximmer.converters

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.biodatageeks.sequila.utils.InternalParams

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CnMopsConverter {

  val targetsNumberByChr: mutable.Map[String, Int] = mutable.LinkedHashMap[String, Int]()
  val targetStartList: ListBuffer[String] = ListBuffer[String]()
  val targetLengthList: ListBuffer[Int] = ListBuffer[Int]()
  var targetsNr = 0

  def convertToCnMopsFormat(targetCountResult: mutable.Map[String, (DataFrame, Long)], outputPath: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sampleNames = targetCountResult.keys.toList
    val samplesValues: ListBuffer[List[String]] = ListBuffer[List[String]]()
    var sampleDFList = targetCountResult.map(x => x._2._1).toList

    val firstSampleValues = fillTargetsInfo(sampleDFList.head)
    samplesValues += firstSampleValues
    sampleDFList = sampleDFList.drop(1)

    for (sampleDF <- sampleDFList) {
      val sampleValue = fillValuesForSample(sampleDF)
      samplesValues += sampleValue
    }

    val chrList = targetsNumberByChr.keys.toList
    val chrNr = chrList.size
    val targetsForEachChromosome = targetsNumberByChr.values.toList

    val singleSampleCoveragesList = samplesValues.toStream
      .map(x => {
        val values = addSquareBrackets(x.mkString(", "))
        singleSampleCoverages.format(values)
      })
      .toList

    this.chromosomeList = addSquareBrackets(addExtraQuotesAndJoinToString(chrList))
    this.chrIterators = addSquareBrackets(Range.inclusive(1, chrNr).toList.mkString(", "))
    this.sampleNames = addSquareBrackets(addExtraQuotesAndJoinToString(sampleNames))
    this.naXChrSize = addSquareBrackets(addExtraQuotesAndJoinToString(List.fill(chrNr)("NA")))
    this.nullXChrSize = addSquareBrackets(List.fill(chrNr)(null).mkString(", "))
    this.targetsNumberAll = addSquareBrackets(targetsNr.toString)
    this.rangeStartList = addSquareBrackets(targetStartList.toList.mkString(", "))
    this.rangeWidthList = addSquareBrackets(targetLengthList.toList.mkString(", "))
    this.targetsNumberPerChr = addSquareBrackets(targetsForEachChromosome.mkString(", "))
    this.sampleCoverageList = addSquareBrackets(singleSampleCoveragesList.mkString(", "))

    val fileName = "analysis.cnmops.cnvs.json"
    val output = outputPath + "/" + fileName
    writeResultJson(output)
    if (spark.conf.get(InternalParams.saveAsSparkFormat).toBoolean) {
      val resultDF = spark.read.option("wholetext", value = true).text(output)
      resultDF.write.text(outputPath + "/spark")
    }
  }

  private def fillValuesForSample(sampleDF: DataFrame): List[String] = {
    sampleDF.collect().map(row => {
      row.getLong(4).toString
    }).toList
  }

  private def fillTargetsInfo(df: DataFrame): List[String] = {
    df.collect().map(row => {
      val chr = row.getString(0)
      val start = row.getString(1)
      val end = row.getString(2)

      if (!targetsNumberByChr.contains(chr)) {
        targetsNumberByChr += (chr -> 0)
      }
      targetsNumberByChr(chr) += 1

      targetStartList += start
      targetLengthList += (end.toInt - start.toInt)

      targetsNr += 1

      row.getLong(4).toString
    }).toList
  }

  private def addSquareBrackets(s: String): String = {
    "[" + s + "]"
  }

  private def addExtraQuotesAndJoinToString(l: List[String]): String = {
    val listWithExtraQuotes = l.toStream
      .map(x => addExtraQuotes(x))
      .toList
    listWithExtraQuotes.mkString(", ")
  }

  private def addExtraQuotes(s: String): String = {
    "\"" + s + "\""
  }

  val singleSampleCoverages: String =
    s"""{
       |  "type": "integer",
       |  "attributes": {},
       |  "value": %s
       |}
       |""".stripMargin

  // Lista chromosomow ["chrX"]
  var chromosomeList: String = ""
  // Tyle ile chromosomow [1, 2, 3]
  var chrIterators: String = ""
  // Ilosc targetow dla kazdego chr [6157, 1, 2]
  var targetsNumberPerChr: String = ""
  // Poczatki targetow [2782017, 155545028]
  var rangeStartList: String = ""
  // Szerokosci kazdego z targetow [218, 188]
  var rangeWidthList: String = ""
  //["NA", "NA", "NA"]
  var naXChrSize: String = ""
  //[null, null, null]
  var nullXChrSize: String = ""
  // ["XI001", "XI002", "XI003", "XI004", "XI005", "XI006", "XI007", "XI008", "XI009", "XI010", "XI011", "XI012", "XI013", "XI014"
  var sampleNames: String = ""
  // Lista singleSampleCoverages - pokrycia dla kazdej probki
  var sampleCoverageList: String = ""
  // Ilosc targetow [15]
  var targetsNumberAll: String = ""

  private def writeResultJson(outputPath: String): Unit = {
    val fileObject = new File(outputPath)
    val pw = new PrintWriter(fileObject)

    val formattedJson: String =
      f"""{
         |  "type": "S4",
         |  "attributes": {
         |    "seqnames": {
         |      "type": "S4",
         |      "attributes": {
         |        "values": {
         |          "type": "integer",
         |          "attributes": {
         |            "levels": {
         |              "type": "character",
         |              "attributes": {},
         |              "value": $chromosomeList
         |            },
         |            "class": {
         |              "type": "character",
         |              "attributes": {},
         |              "value": ["factor"]
         |            }
         |          },
         |          "value": $chrIterators
         |        },
         |        "lengths": {
         |          "type": "integer",
         |          "attributes": {},
         |          "value": $targetsNumberPerChr
         |        },
         |        "elementMetadata": {
         |          "type": "NULL"
         |        },
         |        "metadata": {
         |          "type": "list",
         |          "attributes": {},
         |          "value": []
         |        }
         |      },
         |      "value": {
         |        "class": "Rle",
         |        "package": "S4Vectors"
         |      }
         |    },
         |    "ranges": {
         |      "type": "S4",
         |      "attributes": {
         |        "start": {
         |          "type": "integer",
         |          "attributes": {},
         |          "value": $rangeStartList
         |        },
         |        "width": {
         |          "type": "integer",
         |          "attributes": {},
         |          "value": $rangeWidthList
         |        },
         |        "NAMES": {
         |          "type": "NULL"
         |        },
         |        "elementType": {
         |          "type": "character",
         |          "attributes": {},
         |          "value": ["ANY"]
         |        },
         |        "elementMetadata": {
         |          "type": "NULL"
         |        },
         |        "metadata": {
         |          "type": "list",
         |          "attributes": {},
         |          "value": []
         |        }
         |      },
         |      "value": {
         |        "class": "IRanges",
         |        "package": "IRanges"
         |      }
         |    },
         |    "strand": {
         |      "type": "S4",
         |      "attributes": {
         |        "values": {
         |          "type": "integer",
         |          "attributes": {
         |            "levels": {
         |              "type": "character",
         |              "attributes": {},
         |              "value": ["+", "-", "*"]
         |            },
         |            "class": {
         |              "type": "character",
         |              "attributes": {},
         |              "value": ["factor"]
         |            }
         |          },
         |          "value": [3]
         |        },
         |        "lengths": {
         |          "type": "integer",
         |          "attributes": {},
         |          "value": $targetsNumberAll
         |        },
         |        "elementMetadata": {
         |          "type": "NULL"
         |        },
         |        "metadata": {
         |          "type": "list",
         |          "attributes": {},
         |          "value": []
         |        }
         |      },
         |      "value": {
         |        "class": "Rle",
         |        "package": "S4Vectors"
         |      }
         |    },
         |    "seqinfo": {
         |      "type": "S4",
         |      "attributes": {
         |        "seqnames": {
         |          "type": "character",
         |          "attributes": {},
         |          "value": $chromosomeList
         |        },
         |        "seqlengths": {
         |          "type": "integer",
         |          "attributes": {},
         |          "value": $naXChrSize
         |        },
         |        "is_circular": {
         |          "type": "logical",
         |          "attributes": {},
         |          "value": $nullXChrSize
         |        },
         |        "genome": {
         |          "type": "character",
         |          "attributes": {},
         |          "value": $nullXChrSize
         |        }
         |      },
         |      "value": {
         |        "class": "Seqinfo",
         |        "package": "GenomeInfoDb"
         |      }
         |    },
         |    "elementMetadata": {
         |      "type": "S4",
         |      "attributes": {
         |        "rownames": {
         |          "type": "NULL"
         |        },
         |        "nrows": {
         |          "type": "integer",
         |          "attributes": {},
         |          "value": $targetsNumberAll
         |        },
         |        "listData": {
         |          "type": "list",
         |          "attributes": {
         |            "names": {
         |              "type": "character",
         |              "attributes": {},
         |              "value": $sampleNames
         |            }
         |          },
         |          "value": $sampleCoverageList
         |        },
         |        "elementType": {
         |          "type": "character",
         |          "attributes": {},
         |          "value": ["ANY"]
         |        },
         |        "elementMetadata": {
         |          "type": "NULL"
         |        },
         |        "metadata": {
         |          "type": "list",
         |          "attributes": {},
         |          "value": []
         |        }
         |      },
         |      "value": {
         |        "class": "DFrame",
         |        "package": "S4Vectors"
         |      }
         |    },
         |    "elementType": {
         |      "type": "character",
         |      "attributes": {},
         |      "value": ["ANY"]
         |    },
         |    "metadata": {
         |      "type": "list",
         |      "attributes": {},
         |      "value": []
         |    }
         |  },
         |  "value": {
         |    "class": "GRanges",
         |    "package": "GenomicRanges"
         |  }
         |}""".stripMargin

    pw.write(formattedJson)
    pw.close()
  }
}
