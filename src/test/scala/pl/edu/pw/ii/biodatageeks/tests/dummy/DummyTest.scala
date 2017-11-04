package pl.edu.pw.ii.biodatageeks.tests.dummy

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

/**
  * Created by marek on 04/11/2017.
  */
class DummyTest extends FunSuite with SharedSparkContext{

  test("Sample test"){
    assert(1 === 1)
  }

}
