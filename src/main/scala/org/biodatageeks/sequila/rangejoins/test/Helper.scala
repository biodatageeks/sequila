package org.biodatageeks.sequila.rangejoins.test

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Helper {
  def decimalValue(d: Double, p: Integer): Double = {
    if (d >= 0) {
      BigDecimal(d).setScale(p, BigDecimal.RoundingMode.HALF_UP).toDouble
    } else {
      d
    }
  }

  def readRows(filename: String): ArrayBuffer[SingleRow] = {
    val rows = new ArrayBuffer[SingleRow]()
    for (line <- Source.fromFile(filename).getLines) {
      val values = line.split(",").filterNot(_ == "")
      if (values.length != 1) {
        rows.append(SingleRow(values(0), values(1).toInt, values(2).toInt))
      }
    }
    rows
  }
}
case class SingleRow(chromosome: String, start: Int, end: Int)
