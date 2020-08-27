package org.biodatageeks.sequila.apps

import scala.collection.mutable

object Benchmark {
  val start  = 1
  val stop = 100

  def main(args: Array[String]): Unit = {
    run()
  }

  def prepareMap(nestedMap: mutable.IntMap[mutable.HashMap[Byte, Array[Short]]]): Unit = {
    val map = 'A' -> new Array[Short](90)
    val map2 = 'C' -> new Array[Short](90)

  }

  def run(): Unit = {

    val nestedMap = new mutable.IntMap[mutable.HashMap[Byte, Array[Short]]]()

    prepareMap(nestedMap)


  }
}
