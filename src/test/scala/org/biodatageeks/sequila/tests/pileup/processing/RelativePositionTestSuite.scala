package org.biodatageeks.sequila.tests.pileup.processing

import htsjdk.samtools.{Cigar, CigarElement, CigarOperator}
import org.biodatageeks.sequila.pileup.model.{CigarDerivedConf, ReadQualSummary}
import org.scalatest.FunSuite

import scala.collection.JavaConversions.seqAsJavaList


class RelativePositionTestSuite extends FunSuite{

  test("relative test #1") {
    val cElement1 = new CigarElement(78, CigarOperator.M)
    val cElement2 = new CigarElement(1, CigarOperator.D)
    val cElement3 = new CigarElement(23, CigarOperator.M)
    val c = new Cigar(seqAsJavaList(List(cElement1,cElement2,cElement3)))
    val len = c.getReadLength
    val conf = CigarDerivedConf.create(7801998, c)

    val rs = ReadQualSummary(7801998, 7801998 + len, new Array[Byte](len), conf)

    assert(len==101)
    assert(conf.hasDel)
    assert (rs.relativePosition(7801998) ==0)
    assert (rs.relativePosition(7802099) ==100)
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

    val rs = ReadQualSummary(220108224, 220108224 + len, new Array[Byte](len), conf)

    assert(len==101)
    assert(conf.hasDel)
    assert (rs.relativePosition(220108224) ==0)
    assert (rs.relativePosition(220108325) ==100)
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
    val rs = ReadQualSummary(813652, 813744, new Array[Byte](len), conf)

    assert(len==101)
    assert(conf.hasDel)
    assert (rs.relativePosition(813652) ==0)
    assert (rs.relativePosition(813671) ==19) // 20th base match -> 19th index
    assert (rs.relativePosition(813672) ==19+4+1) // first insert
    assert (rs.relativePosition(813673) ==19+4+1+1)
    assert (rs.relativePosition(813675) ==19+4+1+1+2)
    assert (rs.relativePosition(813676) ==19+4+1+1+2+4+1) // second insert
    assert(rs.relativePosition(813744) == 100) // last index from ref

    //    assert(rs.getBaseQualityForPosition(220108224)==0)
//    assert(rs.getBaseQualityForPosition(220108325)==0)
//    assertThrows[java.lang.ArrayIndexOutOfBoundsException](rs.getBaseQualityForPosition(220108326))

  }

}



