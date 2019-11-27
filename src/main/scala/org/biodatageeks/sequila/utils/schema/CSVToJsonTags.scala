package org.biodatageeks.sequila.utils.schema

import scala.io.Source

object CSVToJsonTags {

  def convertToJson(file:String) ={
    val lines = Source.fromFile(file)
    val iter = lines.getLines().map(_.split(";"))
    iter
      .filter(l => !l.startsWith("#"))
      .filter(l => !l(1).equalsIgnoreCase("?"))
      .foreach{
      l => println(
        s"""|{
          |"name": "tag_${l(0).trim}",
          |"doc": "${l(2).trim}",
          |"type": ["${if (l(1).trim.equalsIgnoreCase("i")) "int" else "string"}", "null"],
          |"default": null
          |},""".stripMargin)
    }

  }
  def main(args: Array[String]): Unit = {
    convertToJson("/Users/marek/Downloads/tags_clean.txt")
  }

}
