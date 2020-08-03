package org.biodatageeks.sequila.tests.pileup

import htsjdk.samtools.{Cigar, CigarElement, CigarOperator}
import scala.collection.JavaConversions._
import org.biodatageeks.sequila.pileup.model.{QualityCache, ReadQualSummary}
import org.scalatest.FunSuite

class QualityCacheTestSuite extends FunSuite{

  test("Qual cache test"){

    val qualCache = new QualityCache(0)
    assert(qualCache.length==0)

    qualCache.resize(2)
    assert(qualCache.length==2)


    val cElement = new CigarElement(89, CigarOperator.M)
    val c = new Cigar(seqAsJavaList(List(cElement)))

    val read0 = ReadQualSummary(10,12,"32423423", c)
    val read1 = ReadQualSummary( 13,14,"44234", c)
    val read2 = ReadQualSummary(19,21,"44234", c)
    val read3 = ReadQualSummary( 31,41,"44234", c)

    //add
    qualCache.addOrReplace(read0)
    qualCache.addOrReplace(read1) // different syntax for adding to cache

    assert(qualCache(0).start==read0.start)
    assert(qualCache(1).start==read1.start)

    qualCache.resize(4)
    assert(qualCache.length==4)

    qualCache.addOrReplace(read2)

    //check previous values exist
    assert(qualCache(0).start==read0.start)
    assert(qualCache(1).start==read1.start)
    assert(qualCache(2).start==read2.start)

  }

}
