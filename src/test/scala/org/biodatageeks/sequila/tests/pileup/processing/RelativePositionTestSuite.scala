package org.biodatageeks.sequila.tests.pileup.processing

import htsjdk.samtools.{Cigar, CigarElement, CigarOperator}
import org.biodatageeks.sequila.pileup.model.{CigarDerivedConf, InDelPositions, ReadSummary}
import org.biodatageeks.sequila.rangejoins.methods.IntervalTree.IntervalTreeRedBlack
import org.scalatest.FunSuite
import org.sparkproject.guava.collect.Iterators

import scala.collection.JavaConversions.seqAsJavaList


class RelativePositionTestSuite extends FunSuite{

  test("relative test #1") {
    val cElement1 = new CigarElement(78, CigarOperator.M)
    val cElement2 = new CigarElement(1, CigarOperator.D)
    val cElement3 = new CigarElement(23, CigarOperator.M)
    val c = new Cigar(seqAsJavaList(List(cElement1,cElement2,cElement3)))
    val len = c.getReadLength
    val conf = CigarDerivedConf.create(7801998, c)

    val rs = ReadSummary(7801998, 7801998 + len, new Array[Byte](len), new Array[Byte](len), true, c)

    assert(len==101)
    assert(conf.hasDel)
    assert (rs.expensiveRelativePosition(7801998) ==0)
    assert (rs.expensiveRelativePosition(7802099) ==100)
    assert(rs.getBaseQualityForPosition(7801998)==0)
    assert(rs.getBaseQualityForPosition(7801999)==0)
    assert(rs.getBaseQualityForPosition(7802099)==0)
    assertThrows[java.lang.ArrayIndexOutOfBoundsException](rs.getBaseQualityForPosition(7802100))

  }

  test("relative test #2") {
    val cElement1 = new CigarElement(82, CigarOperator.M)
    val cElement2 = new CigarElement(1, CigarOperator.D)
    val cElement3 = new CigarElement(19, CigarOperator.M)
    val c = new Cigar(seqAsJavaList(List(cElement1,cElement2,cElement3)))
    val len = c.getReadLength

    val conf = CigarDerivedConf.create(220108224, c)

    val rs = ReadSummary(220108224, 220108224 + len, new Array[Byte](len), new Array[Byte](len), true, c)

    assert(len==101)
    assert(conf.hasDel)
    assert (rs.expensiveRelativePosition(220108224) ==0)
    assert (rs.expensiveRelativePosition(220108325) ==100)
    assert(rs.getBaseQualityForPosition(220108224)==0)
    assert(rs.getBaseQualityForPosition(220108325)==0)
    assertThrows[java.lang.ArrayIndexOutOfBoundsException](rs.getBaseQualityForPosition(220108326))

  }

  test("relative test #3") {

    val cElement1 = new CigarElement(20, CigarOperator.M)
    val cElement2 = new CigarElement(4, CigarOperator.I)
    val cElement3 = new CigarElement(4, CigarOperator.M)
    val cElement4 = new CigarElement(4, CigarOperator.I)
    val cElement5 = new CigarElement(33, CigarOperator.M)
    val cElement6 = new CigarElement(1, CigarOperator.D)
    val cElement7 = new CigarElement(6, CigarOperator.M)
    val cElement8 = new CigarElement(1, CigarOperator.I)
    val cElement9 = new CigarElement(29, CigarOperator.M)

    val c = new Cigar(seqAsJavaList(List(cElement1,cElement2,cElement3, cElement4,cElement5,cElement6,cElement7, cElement8,cElement9 )))
    val len = c.getReadLength
    val conf = CigarDerivedConf.create(813652, c)
    val rs = ReadSummary(813652, 813744, new Array[Byte](len), new Array[Byte](len), true, c)

    assert(len==101)
    assert(conf.hasDel)
    assert (rs.expensiveRelativePosition(813652) ==0)
    assert (rs.expensiveRelativePosition(813671) ==19) // 20th base match -> 19th index
    assert (rs.expensiveRelativePosition(813672) ==19+4+1) // first insert
    assert (rs.expensiveRelativePosition(813673) ==19+4+1+1)
    assert (rs.expensiveRelativePosition(813675) ==19+4+1+1+2)
    assert (rs.expensiveRelativePosition(813676) ==19+4+1+1+2+4+1) // second insert
    assert(rs.expensiveRelativePosition(813744) == 100) // last index from ref

  }
  test("relative test #4") {

    val cElement1 = new CigarElement(6, CigarOperator.S)
    val cElement2 = new CigarElement(6, CigarOperator.M)
    val cElement3 = new CigarElement(37, CigarOperator.D)
    val cElement4 = new CigarElement(89, CigarOperator.M)


    val c = new Cigar(seqAsJavaList(List(cElement1,cElement2,cElement3, cElement4 )))
    val len = c.getReadLength
    val conf = CigarDerivedConf.create(76032260, c)
    val rs = ReadSummary(76032260, 76032391, new Array[Byte](len), new Array[Byte](len), true, c)

    assert(len==101)
    assert(conf.hasDel)
    assert (rs.expensiveRelativePosition(76032260) == 6)
    assert (rs.expensiveRelativePosition(76032261) == 7)
    assert (rs.expensiveRelativePosition(76032262) == 8)
    assert (rs.expensiveRelativePosition(76032263) == 9)
    assert (rs.expensiveRelativePosition(76032264) == 10)
    assert (rs.expensiveRelativePosition(76032265) == 11)
    assert(rs.hasDeletionOnPosition(76032266))
  }

  test("relative test #5") {

    val cElement1 = new CigarElement(3, CigarOperator.S) // 3S
    val cElement2 = new CigarElement(14, CigarOperator.M) // 14M
    val cElement3 = new CigarElement(4, CigarOperator.D) // 4D
    val cElement4 = new CigarElement(14, CigarOperator.M) // 14M
    val cElement5 = new CigarElement(4, CigarOperator.D) // 4D
    val cElement6 = new CigarElement(14, CigarOperator.M) // 14M

    val cElement7 = new CigarElement(4, CigarOperator.D) // 4D
    val cElement8 = new CigarElement(14, CigarOperator.M) // 14M
    val cElement9 = new CigarElement(4, CigarOperator.D) //  4D
    val cElement10 = new CigarElement(14, CigarOperator.M) // 14M

    val cElement11 = new CigarElement(4, CigarOperator.D) // 4D
    val cElement12 = new CigarElement(14, CigarOperator.M) // 14M
    val cElement13 = new CigarElement(3, CigarOperator.D) // 3D
    val cElement14 = new CigarElement(9, CigarOperator.M) // 9M
    val cElement15 = new CigarElement(5, CigarOperator.S) // 5S

    val c = new Cigar(seqAsJavaList(List(
            cElement1,
            cElement2,
            cElement3,
            cElement4,
            cElement5,
            cElement6,
            cElement7,
            cElement8,
            cElement9,
            cElement10,
            cElement11,
            cElement12,
            cElement13,
            cElement14,
            cElement15 )))
    val len = c.getReadLength
    val conf = CigarDerivedConf.create(2, c)
    val rs = ReadSummary(2, 117, new Array[Byte](len), new Array[Byte](len), true, c)

//    assert(len==101)
//    assert(conf.hasDel)
//    assert (rs.relativePosition(104) < 101 )
//    assert (rs.relativePosition(117) < 101 )


  }

  test("relative test #6") {

    val cElement1 = new CigarElement(8271, CigarOperator.H)
    val cElement2 = new CigarElement(10, CigarOperator.M)


    val c = new Cigar(seqAsJavaList(List(cElement1,cElement2 )))
    val len = c.getReadLength
    val conf = CigarDerivedConf.create(10001, c)
    val rs = ReadSummary(10001, 10011, new Array[Byte](len), new Array[Byte](len), true, c)

    assert(len==10)
    assert (rs.expensiveRelativePosition(10001) == 0)
    assert (rs.expensiveRelativePosition(10002) == 1)
  }

}



