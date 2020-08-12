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

}



