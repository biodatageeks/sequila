package org.biodatageeks.sequila.pileup.converters

import scala.collection.mutable

object PileupStringUtils {

  val beginOfReadMark = '^'
  val endOfReadMark = '$'

  val insertionMark = '+'
  val deletionMark = '-'

  val refMatchPlusStrand = '.'
  val refMatchMinuStrand = ','

  def removeAllMarks(inputPileup: String): String = {
    val noStartNoEndPileup = removeStartAndEndMarks(inputPileup)
    val noIndelsPileup = removeIndels(noStartNoEndPileup)
    noIndelsPileup
  }

  def removeStartAndEndMarks(inputPileup:String):String = {
    val noStartsPileup = removeStartOfReadMark(inputPileup)
    val noStartNoEndPileup = removeEndOfReadMark(noStartsPileup)
    noStartNoEndPileup
  }

  def removeEndOfReadMark(inputPileup: String): String = inputPileup.filterNot(Set(endOfReadMark))

  def removeStartOfReadMark(inputPileup:String): String = {
    val buf = new StringBuilder()
    var index = 0
    while (index < inputPileup.length){
      val char = inputPileup(index)
      if (char != beginOfReadMark)
        buf.append(char)
      else if (char == beginOfReadMark)
        index += 1
      index += 1
    }
    buf.toString
  }

  def removeIndels(inputPileup: String): String = {
    if (!inputPileup.contains(insertionMark) && !inputPileup.contains(deletionMark))
      return (inputPileup)

    var outputPileup = inputPileup
    var index = 0

    while (index <= outputPileup.length-1) {
      val char = outputPileup.charAt(index)
      if (char == insertionMark || char == deletionMark){
        val (delLen, charLen) = delLength(outputPileup, index)
        outputPileup = outputPileup.slice(0, index) ++ outputPileup.slice(index+delLen+1+charLen, outputPileup.length)
      }
      index +=1
    }
    outputPileup
  }

  private def delLength(pileup: String, ind: Int) = {
    val buf = new mutable.StringBuilder()
    buf.append(pileup.charAt(ind + 1) - '0')
    if (pileup.charAt(ind + 2).isDigit ) {
      buf.append(pileup.charAt(ind + 2) - '0')
      if (pileup.charAt(ind + 3).isDigit)
        buf.append(pileup.charAt(ind + 3) - '0')
    }
    (buf.toString().toInt, buf.length)
  }

  def getBaseCountMap(pileup: String): mutable.Map[String, Int] = {
    val basesCount = new mutable.HashMap[String, Int]()
    basesCount("A") = pileup.count(_ == 'A')
    basesCount("C") = pileup.count(_ == 'C')
    basesCount("G") = pileup.count(_ == 'G')
    basesCount("T") = pileup.count(_ == 'T')
    basesCount("N") = pileup.count(_ == 'N')
    basesCount("a") = pileup.count(_ == 'a')
    basesCount("c") = pileup.count(_ == 'c')
    basesCount("g") = pileup.count(_ == 'g')
    basesCount("t") = pileup.count(_ == 't')
    basesCount("n") = pileup.count(_ == 'n')
    basesCount

  }

}
