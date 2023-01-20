package org.biodatageeks.sequila.tests.ximmer.converters

import org.biodatageeks.sequila.ximmer.converters.ConiferConverter

/**
  * Testujemy tylko formatowanie. Oczekiwane pokrycie zostało lekko zmodyfikowane
  * Ponieważ oryginalny conifer błędnie liczy ilość odczytów w próbce, wyliczona wartość
  * została zmodyfikowana do poprawnej ilości odczytów w próbce
  */
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