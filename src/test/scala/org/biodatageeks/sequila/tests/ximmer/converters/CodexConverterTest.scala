package org.biodatageeks.sequila.tests.ximmer.converters

import org.biodatageeks.sequila.ximmer.converters.CodexConverter
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}

class CodexConverterTest extends XimmerConverterTestBase {

  test("Convert to codex format") {
    //given
    val expectedChr1 = scala.io.Source.fromFile(ximmerResourceDir + "/codex/expected.codex_coverage.chr1.json")
    val expectedChr2 = scala.io.Source.fromFile(ximmerResourceDir + "/codex/expected.codex_coverage.chr2.json")
    val expectedChrX = scala.io.Source.fromFile(ximmerResourceDir + "/codex/expected.codex_coverage.chrX.json")
    //when
    new CodexConverter().convertToCodexFormat(targetCountsResult, tempDir)
    //then
    val resultChr1 = scala.io.Source.fromFile(tempDir + "/analysis.codex_coverage.chr1.json")
    val resultChr2 = scala.io.Source.fromFile(tempDir + "/analysis.codex_coverage.chr2.json")
    val resultChrX = scala.io.Source.fromFile(tempDir + "/analysis.codex_coverage.chrX.json")

    JSONAssert.assertEquals(expectedChr1.mkString, resultChr1.mkString, JSONCompareMode.NON_EXTENSIBLE)
    JSONAssert.assertEquals(expectedChr2.mkString, resultChr2.mkString, JSONCompareMode.NON_EXTENSIBLE)
    JSONAssert.assertEquals(expectedChrX.mkString, resultChrX.mkString, JSONCompareMode.NON_EXTENSIBLE)

    expectedChr1.close(); expectedChr2.close(); expectedChrX.close()
    resultChr1.close(); resultChr2.close(); resultChrX.close()
  }

}
