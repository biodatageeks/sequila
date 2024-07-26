package org.biodatageeks.sequila.apps

import org.apache.spark.sql.types.StructType

object ITApp extends App with SequilaApp {
  override def main(args: Array[String]): Unit = {

    val ss = createSequilaSession()
      ss.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder", "true")

    val chainRn4_chr1 = ss
      .read
      .option("delimiter", "\t")
      .option("header", true)
      .schema(StructType.fromDDL("column0 string, column1 int, column2 int"))
      .csv("/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainRn4_chr1.csv")

    chainRn4_chr1.createOrReplaceTempView("chainRn4_chr1")
    val chainVicPac2 = ss
      .read
      .option("delimiter", "\t")
      .option("header", true)
      .schema(StructType.fromDDL("column0 string, column1 int, column2 int"))
      .csv("/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainVicPac2_chr1.csv")
    chainVicPac2.createOrReplaceTempView("chainVicPac2_chr1")
    ss.time{
      ss
        .sql("select count(*) from chainRn4_chr1 a, chainVicPac2_chr1 b where (a.column0=b.column0 and a.column2>=b.column1 and a.column1<=b.column2);")
        .show()
    }
    ss.stop()
  }
}
