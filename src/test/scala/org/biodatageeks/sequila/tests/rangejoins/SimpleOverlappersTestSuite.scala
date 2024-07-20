package org.biodatageeks.sequila.tests.rangejoins

import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeRedBlack
import org.scalatest.funsuite.AnyFunSuite
import org.sparkproject.guava.collect.Iterators


class SimpleOverlappersTestSuite extends AnyFunSuite {

  test("overlappers without end #13") {

    val dels = new IntervalTreeRedBlack[(Int)]()
    dels.put(1, 1, 2) // (10539,10541,[2],21,10701,true)
    dels.put(2, 2, 1) // (10550,10551,[1],3,10561,true)
    dels.put(3, 3, 1) // (10550,10551,[1],3,10561,true)
    dels.put(4, 4, 6) // (10508,10514,[6],7,10537,false)
    dels.put(5, 5, 2) // (10125,10127,[2],3,10256,true)
    dels.put(6, 6, 1) //(10034,10035,[1],1,10035,true)
    dels.put(7, 7, 2) //(10254,10256,[2],1,10256,true)
    dels.put(8, 8, 4) //(10530,10534,[4],3,10537,true)
    dels.put(9, 9, 4) //(10530,10534,[4],3,10537,true)
    dels.put(11, 11, 4) //(10530,10534,[4],3,10537,true)
    dels.put(10, 10, 4) //(10530,10534,[4],3,10537,true)

    dels.printTree()
    var it = dels.overlappersWithoutEnd(1)

     it = dels.overlappersWithoutEnd(1)
    assert (Iterators.size(it) == 1)

    it = dels.overlappersWithoutEnd(2)
    assert (Iterators.size(it) == 2)

    it = dels.overlappersWithoutEnd(3)
    assert (Iterators.size(it) == 3)

    it = dels.overlappersWithoutEnd(4)
    assert (Iterators.size(it) == 4)

    it = dels.overlappersWithoutEnd(5)
    assert (Iterators.size(it) == 5)

    it = dels.overlappersWithoutEnd(6)
    assert (Iterators.size(it) == 6)

    it = dels.overlappersWithoutEnd(7)
    assert (Iterators.size(it) == 7)



  }
}