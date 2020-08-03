package org.biodatageeks.sequila.tests.pileup.processing

import org.biodatageeks.sequila.pileup.converters.PileupStringUtils
import org.scalatest.FunSuite

class PileupUtilsTest extends FunSuite{

  test("clean Pileup") {

    var rawPileup = "...,"
    var expectedPileup = "...,"
    var cleanedPileup = PileupStringUtils.removeAllMarks(rawPileup)
    assert(expectedPileup==cleanedPileup)

    rawPileup = "$A..$.,$"
    expectedPileup = "A...,"
    cleanedPileup = PileupStringUtils.removeAllMarks(rawPileup)
    assert(expectedPileup===cleanedPileup)

    rawPileup = "$A^!..$.,$"
    expectedPileup = "A...,"
    cleanedPileup = PileupStringUtils.removeAllMarks(rawPileup)
    assert(expectedPileup===cleanedPileup)

    rawPileup = "^~."
    expectedPileup = "."
    cleanedPileup = PileupStringUtils.removeAllMarks(rawPileup)
    assert(expectedPileup==cleanedPileup)


    rawPileup = "^~.^$"
    expectedPileup = "."
    cleanedPileup = PileupStringUtils.removeAllMarks(rawPileup)
    assert(expectedPileup==cleanedPileup)

  }

}
