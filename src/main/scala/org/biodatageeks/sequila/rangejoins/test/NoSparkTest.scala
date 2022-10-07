package org.biodatageeks.sequila.rangejoins.test

import org.biodatageeks.sequila.rangejoins.methods.base.BaseIntervalHolder

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object NoSparkTest {

  def main(args: Array[String]): Unit = {
    val denominator = 1000000000
    val treeFileName = args(0)
    val datasetFileName = args(1)
    val className = args(2)
    val rows = Helper.readRows(treeFileName)
    val clazz = Class.forName(className)
    val structures = new mutable.HashMap[String, BaseIntervalHolder[Any]] {}
    val df1: scala.collection.Map[String, ArrayBuffer[SingleRow]] = rows.groupBy(row => row.chromosome)
    val t0 = System.nanoTime()
    df1.foreach(tuple => {
      val list = clazz.getConstructor().newInstance().asInstanceOf[BaseIntervalHolder[Any]]
      tuple._2.foreach(sr => {
        list.put(sr.start, sr.end, null)
      })
      list.postConstruct(Option.empty[Int])
      structures.put(tuple._1, list)
    })
    val t1 = System.nanoTime()
    println("Building structure time=" + Helper.decimalValue((t1 - t0).toDouble / denominator, 2))
    var counter = 0L
    val rows2 = Helper.readRows(datasetFileName)
    val t2 = System.nanoTime()
    rows2.foreach(x => {
      if (structures.contains(x.chromosome)) {
        val list = structures(x.chromosome)
        list.overlappers(x.start, x.end).forEachRemaining(p => {
          counter = counter + p.operations
        })
      }
    })
    val t3 = System.nanoTime()
    println("                  count=" + counter)
    println("       Overlapping time=" + Helper.decimalValue((t3 - t2).toDouble / denominator, 2))
    println("                    Sum=" + Helper.decimalValue(((t3 - t2).toDouble + (t1 - t0).toDouble) / denominator, 2))
  }
}
