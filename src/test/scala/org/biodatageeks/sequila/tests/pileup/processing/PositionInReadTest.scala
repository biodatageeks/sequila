package org.biodatageeks.sequila.tests.pileup.processing

import htsjdk.samtools.{Cigar, CigarElement, CigarOperator}
import org.biodatageeks.sequila.pileup.model.{ExtendedReads, PositionInReadState}
import org.scalatest.FunSuite

import scala.collection.JavaConversions.seqAsJavaList

class PositionInReadTest extends FunSuite{

  /** take into account one insert */
  test("position in read test #1") {
    val cElement1 = new CigarElement(10, CigarOperator.M)
    val cElement2 = new CigarElement(1, CigarOperator.I)
    val cElement3 = new CigarElement(23, CigarOperator.M)
    val c = new Cigar(seqAsJavaList(List(cElement1,cElement2,cElement3)))


    val testRead = ExtendedReads(null)
    val insertions = testRead.calculateInsertions(c)
    val posisitonState = PositionInReadState(0,0)
    val ind = testRead.calculatePositionInReadSeq(10, insertions, posisitonState).position
    assert(ind==11)
    val ind2 = testRead.calculatePositionInReadSeq(20,insertions, posisitonState).position
    assert(ind2==21)
  }

  /** Hard clip, no inserts */
  test("position in read test #2") {
    val cElement1 = new CigarElement(10, CigarOperator.H)
    val cElement2 = new CigarElement(1, CigarOperator.D)
    val cElement3 = new CigarElement(23, CigarOperator.M)
    val c = new Cigar(seqAsJavaList(List(cElement1,cElement2,cElement3)))

    val testRead = ExtendedReads(null)
    val insertions = testRead.calculateInsertions(c)
    val posisitonState = PositionInReadState(0,0)
    val ind = testRead.calculatePositionInReadSeq(10, insertions, posisitonState).position
    assert(ind==10)
    val ind2 = testRead.calculatePositionInReadSeq(23, insertions, posisitonState).position
    assert(ind2==23)
  }

  /** Hard clip, take into account insert */
  test("position in read test #3") {
    val cElement1 = new CigarElement(10, CigarOperator.H)
    val cElement2 = new CigarElement(1, CigarOperator.I)
    val cElement3 = new CigarElement(23, CigarOperator.M)
    val c = new Cigar(seqAsJavaList(List(cElement1,cElement2,cElement3)))
    print (c.getReadLength)

    val testRead = ExtendedReads(null)
    val insertions = testRead.calculateInsertions(c)
    val posisitonState = PositionInReadState(0,0)
    val ind = testRead.calculatePositionInReadSeq(10,insertions, posisitonState).position
    assert(ind==11)
    val ind2 = testRead.calculatePositionInReadSeq(23,insertions, posisitonState).position
    assert(ind2==24)
  }

}
