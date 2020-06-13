package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.SharedSparkContext
import org.biodatageeks.sequila.pileup.{MDTagParser, PileupMethods}
import org.biodatageeks.sequila.tests.base.BAMBaseTestSuite
import org.biodatageeks.sequila.utils.FastMath

class CovArrayTestSuite  extends BAMBaseTestSuite with SharedSparkContext{

  test("Last index performance"){

    val th = ichi.bench.Thyme.warmed(verbose = print)

    val xArrZeroes = Array.fill[Int](10)(0)
    val xArr = -10000 to 10000 toArray

    val testArrI = xArrZeroes ++ xArr ++ xArrZeroes ++ xArr ++ xArrZeroes
    val testArr = testArrI.map(_.toShort)

    th.pbench(testArr.lastIndexWhere(x=> x!=0))

    th.pbench(FastMath.findMaxIndex(testArr))

  }



}
