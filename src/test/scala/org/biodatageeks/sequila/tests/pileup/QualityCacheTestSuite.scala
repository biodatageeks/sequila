package org.biodatageeks.sequila.tests.pileup

import htsjdk.samtools.{Cigar, CigarElement, CigarOperator}

import scala.collection.JavaConversions._
import org.biodatageeks.sequila.pileup.model.{CigarDerivedConf, QualityCache, ReadSummary}
import org.scalatest.FunSuite

class QualityCacheTestSuite extends FunSuite{

  test("Qual cache test"){

    val qualCache = new QualityCache(4)
    assert(qualCache.length==8)



    val cElement = new CigarElement(89, CigarOperator.M)
    val c = new Cigar(seqAsJavaList(List(cElement)))
    val cigarConf0 = CigarDerivedConf.create(10, c)
    val read0 = ReadSummary(10,12,Array[Byte](3,2,4,2,3,4,2,3), Array[Byte](3,2,4,2,3,4,2,3), cigarConf0)
    val cigarConf1 = CigarDerivedConf.create(13, c)
    val read1 = ReadSummary( 13,14,Array[Byte](3,2,4,2,3,4,2,3), Array[Byte](3,2,4,2,3,4,2,3), cigarConf1)
    val cigarConf2 = CigarDerivedConf.create(19, c)
    val read2 = ReadSummary(19,21,Array[Byte](3,2,4,2,3,4,2,3), Array[Byte](3,2,4,2,3,4,2,3), cigarConf2)
    val cigarConf3 = CigarDerivedConf.create(31, c)
    val read3 = ReadSummary( 31,41,Array[Byte](3,2,4,2,3,4,2,3), Array[Byte](3,2,4,2,3,4,2,3), cigarConf3)

    //add
    qualCache.addOrReplace(read0)
    qualCache.addOrReplace(read1) // different syntax for adding to cache

    assert(qualCache(0).start==read0.start)
    assert(qualCache(1).start==read1.start)


    qualCache.addOrReplace(read2)

    //check previous values exist
    assert(qualCache(0).start==read0.start)
    assert(qualCache(1).start==read1.start)
    assert(qualCache(2).start==read2.start)

  }

}
