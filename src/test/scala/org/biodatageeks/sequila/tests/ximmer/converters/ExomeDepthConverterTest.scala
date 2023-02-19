/**
  * Created by Krzysztof Kobyliński
  */
package org.biodatageeks.sequila.tests.ximmer.converters

import org.biodatageeks.sequila.ximmer.converters.ExomeDepthConverter

class ExomeDepthConverterTest extends XimmerConverterTestBase {

  test("Convert to exomeDepth format") {
    //given
    val expectedChr1Json = scala.io.Source.fromFile(ximmerResourceDir + "/exomedepth/expected.chr1.counts.tsv")
    val expectedChr2Json = scala.io.Source.fromFile(ximmerResourceDir + "/exomedepth/expected.chr2.counts.tsv")
    val expectedChrXJson = scala.io.Source.fromFile(ximmerResourceDir + "/exomedepth/expected.chrX.counts.tsv")
    //when
    new ExomeDepthConverter().convertToExomeDepthFormat(targetCountsResult, tempDir)
    //then
    val resultChr1 = scala.io.Source.fromFile(tempDir + "/analysis.chr1.counts.tsv")
    val resultChr2 = scala.io.Source.fromFile(tempDir + "/analysis.chr2.counts.tsv")
    val resultChrX = scala.io.Source.fromFile(tempDir + "/analysis.chrX.counts.tsv")

    assert(expectedChr1Json.getLines().mkString == resultChr1.getLines().mkString)
    assert(expectedChr2Json.getLines().mkString == resultChr2.getLines().mkString)
    assert(expectedChrXJson.getLines().mkString == resultChrX.getLines().mkString)

    expectedChr1Json.close(); expectedChr2Json.close(); expectedChrXJson.close()
    resultChr1.close(); resultChr2.close(); resultChrX.close()
  }

}

