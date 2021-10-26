package org.biodatageeks.sequila.tests.pileup.model

import org.scalatest.FunSuite
import org.biodatageeks.sequila.pileup.model.Quals._

class QualsTestSuite extends FunSuite{
  val shift = 'A'
//
//  test ("merge test") {
//    val quals1 = new Array[Array[Short]](30)
//    val quals2 = new Array[Array[Short]](30)
//
//    quals1('A'- shift) = Array(1,1,1,1,1)
//    quals1('C'- shift) = Array(1,1,1,1,1)
//
//    quals2('A'- shift) = Array(10,10,10,10,10)
//    quals1('T'- shift) = Array(20,20,20,20,20)
//
//    val res = quals1.merge(quals2)
//
//    assert(res('A' - shift) sameElements Array(11, 11, 11, 11, 11))
//    assert(res('C' - shift) sameElements Array(1,1,1,1,1))
//    assert(res('G' - shift) == null)
//    assert(res('T' - shift) sameElements Array(20,20,20,20,20))
//
//  }
//
//  test ("derived coverage test") {
//    val quals = new Array[Array[Short]](30)
//    quals('A'- shift) = Array(1,1,1,1,1)
//    quals('C'- shift) = Array(1,2,10)
//
//    val res = quals.derivedCoverage
//    assert(res==18)
//
//  }

}
