package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.SharedSparkContext
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.tests.base.BAMBaseTestSuite

class MDTagTestSuite extends BAMBaseTestSuite with SharedSparkContext{

  val mgTagPath: String = getClass.getResource("/md/md_tags.txt.gz").getPath
//  test("MD Tag parser performance"){
//
//    val th = ichi.bench.Thyme.warmed(verbose = print)
//
//    val tags = spark
//      .sparkContext
//      .textFile(mgTagPath)
//        .collect()
//
//    th.pbench(
//    tags
//      .map(MDTagParser.parseMDTag(_)))
//
//  }

}
