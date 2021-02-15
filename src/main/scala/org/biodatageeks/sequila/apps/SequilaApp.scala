package org.biodatageeks.sequila.apps

import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.biodatageeks.sequila.utils.SequilaRegister

trait SequilaApp {
  def createSequilaSession(): SequilaSession = {
    System.setProperty("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.driver.memory","4g")
      .getOrCreate()

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    spark.sparkContext.setLogLevel("WARN")
    ss
  }
}
