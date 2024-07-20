package org.biodatageeks.sequila.tests.pileup.processing

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class IntMapTestSuite extends AnyFunSuite{

  test ("iIntMap") {
    val map = new mutable.IntMap[String]()
    map.update(11, "eleven")
    map.update(0, "zero")
    map.update(Int.MaxValue, "max int")
    assert(map.size == 3)
  }

}
