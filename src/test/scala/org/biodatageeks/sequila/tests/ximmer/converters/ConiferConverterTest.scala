/**
  * Created by Krzysztof Kobyliński
  */
package org.biodatageeks.sequila.tests.ximmer.converters

import org.biodatageeks.sequila.ximmer.converters.ConiferConverter

class ConiferConverterTest extends XimmerConverterTestBase {

  test("Convert to conifer format") {
    //given
    val expected = scala.io.Source.fromFile(ximmerResourceDir + "/conifer/expected.XI001.rpkm")
    //when
    new ConiferConverter().convertToConiferFormat(targetCountsResult.head, tempDir)
    //then
    val result = scala.io.Source.fromFile(tempDir + "/XI001.rpkm")

    assert(expected.getLines().mkString == result.getLines().mkString)

    expected.close()
    result.close()
  }

}