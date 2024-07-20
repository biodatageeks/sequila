package org.biodatageeks.sequila.tests.pileup.processing

import org.biodatageeks.sequila.pileup.converters.samtools.PileupStringUtils

import org.scalatest.funsuite.AnyFunSuite

class PileupUtilsTest extends AnyFunSuite{

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
