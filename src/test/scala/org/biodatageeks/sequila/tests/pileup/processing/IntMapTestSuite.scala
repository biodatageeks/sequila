package org.biodatageeks.sequila.tests.pileup.processing

import org.scalatest.FunSuite

import scala.collection.mutable

class IntMapTestSuite extends FunSuite{

  test ("iIntMap") {
    val map = new mutable.IntMap[String]()
    map.update(11, "eleven")
    map.update(0, "zero")
    map.update(Int.MaxValue, "max int")
    println(map)
    println(map.size)
  }

}
