package pl.edu.pw.ii.biodatageeks.tests

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.biodatageeks.datasources.BAM.BAMRecord
import org.scalatest.{BeforeAndAfter, FunSuite}

class CoverageTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{

  val bamPath = getClass.getResource("/NA12878.slice.bam").getPath

  test ("BAMDatasource to dataset"){

    import spark.implicits._
    val dataset = spark
      .read
      .format("org.biodatageeks.datasources.BAM.BAMDataSource")
      .load(bamPath)
      .as[BAMRecord]

    dataset
      .show()



  }
}
