/**
  * Created by Krzysztof Kobyliński
  */
package org.biodatageeks.sequila.tests.ximmer.converters

import org.biodatageeks.sequila.ximmer.converters.CnMopsConverter
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}

class CnMopsConverterTest extends XimmerConverterTestBase {

  test("Convert to cnmops format") {
    //given
    val expectedJson = scala.io.Source.fromFile(ximmerResourceDir + "/cnmops/expected.cnmops.json")
    //when
    new CnMopsConverter().convertToCnMopsFormat(targetCountsResult, tempDir)
    //then
    val result = scala.io.Source.fromFile(tempDir + "/analysis.cnmops.cnvs.json")

    JSONAssert.assertEquals(expectedJson.mkString, result.mkString, JSONCompareMode.NON_EXTENSIBLE)

    expectedJson.close()
    result.close()
  }
}
