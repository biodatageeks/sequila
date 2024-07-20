package org.biodatageeks.sequila.tests.dataquality

import org.biodatageeks.sequila.utils.DataQualityFuncs
import org.scalatest.funsuite.AnyFunSuite

class ContigNormalizationTest extends AnyFunSuite{

  test("Test contig") {
    val chrInTest1 = "chr1"
    val chrInTest2 = "chrM"
    val chrInTest3 = "M"
    assert(
      DataQualityFuncs.cleanContig(chrInTest1) == "1"
    )
    assert(
      DataQualityFuncs.cleanContig(chrInTest2) === "MT"
    )
    assert(
      DataQualityFuncs.cleanContig(chrInTest3) === "MT"
    )
  }

}
