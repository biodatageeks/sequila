package pl.edu.pw.ii.biodatageeks.tests

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.bdgenomics.utils.instrumentation.Metrics
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.rangejoins.methods.transformations.RangeMethods
import org.scalatest.{BeforeAndAfter, FunSuite}

class GRangesTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{



  val schema = StructType(Seq(StructField("chr",StringType ),StructField("start",IntegerType ), StructField("end", IntegerType)))


  before{
    System.setSecurityManager(null)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    Metrics.initialize(sc)
    val rdd1 = sc
      .textFile(getClass.getResource("/refFlat.txt.bz2").getPath)
      .map(r=>r.split('\t'))
      .map(r=>Row(
        (r(2).toString),
        r(4).toInt,
        r(5).toInt
      ))
    val ref = spark
      .createDataFrame(rdd1,schema)
    ref.createOrReplaceTempView("ref")

    val rdd2 = sc
      .textFile(getClass.getResource("/snp150Flagged.txt.bz2").getPath)
      .map(r=>r.split('\t'))
      .map(r=>Row(
        (r(1).toString),
        r(2).toInt,
        r(3).toInt
      ))
    val snp = spark
      .createDataFrame(rdd2,schema)
    snp.createOrReplaceTempView("snp")




  }

  test("Basic operation - test raw counts"){
    val queryRef =
      """
        |SELECT * FROM ref
      """.stripMargin

    val querySnp =
      """
        |SELECT * FROM snp
      """.stripMargin

    assert( spark.sql(queryRef).count() === 74734L)

    assert( spark.sql(querySnp).count() === 203915L)

  }

  test("Basic operation - test overlap counts"){
    val query =
      s"""
         |SELECT snp.*,ref.* FROM snp JOIN ref
         |ON (ref.chr=snp.chr
         |AND
         |snp.end>=ref.start
         |AND
         |snp.start<=ref.end
         |)
         |
       """.stripMargin
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")
    assert(spark.sql(query).count === 616404L)
  }

  test("Basic operation - test overlap counts with min overlap"){
    val query =
      s"""
         |SELECT ref.*,snp.* FROM snp JOIN ref
         |ON (ref.chr=snp.chr
         |AND
         |snp.end>=ref.start
         |AND
         |snp.start<=ref.end
         |)
         |
       """.stripMargin

    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","10")
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")
    assert(spark.sql(query).count === 7923L)
  }

  test("Basic operation - test overlap counts with max gap") {

//ref.*,snp.*
    val query =
      s"""
         |SELECT ref.*,snp.chr as chr2, snp.start as start2, snp.end as end2 FROM snp JOIN ref
         |ON (ref.chr=snp.chr
         |AND
         |snp.end>=ref.start
         |AND
         |snp.start<=ref.end
         |)
         |
       """.stripMargin

    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","10000")
    assert(spark.sql(query).count === 804488L)

  }

  test("Basic operation - subsetByOverlaps") {

    val query =
      s"""
         |SELECT * FROM ref a,
         |(SELECT distinct ref.* FROM ref JOIN snp
         |ON (ref.chr=snp.chr
         |AND
         |snp.end>=ref.start
         |AND
         |snp.start<=ref.end
         |) ) b
         |WHERE a.chr = b.chr AND a.start=b.start AND a.end = b.end
       """.stripMargin
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")
    assert(spark.sql(query).count === 35941L)
  }

  test( "Basic operation - shift"){
    spark.sqlContext.udf.register("shift", RangeMethods.shift _)
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")

    val query =
      """
        |SELECT chr,start,end,shift(start,5) as start_2 ,shift(end,5) as end_2 FROM ref LIMIT 1
      """.stripMargin
    assert(spark.sql(query).select("start_2").first().get(0) === 11878 && spark.sql(query).select("end_2").first().get(0) === 14414)

  }

  test( "Basic operation - resize - fix=center"){
    spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    spark.sqlContext.setConf("minOverlap","1")
    spark.sqlContext.setConf("maxGap","0")

    val query =
      """
        |SELECT chr,start,end,resize(start,end,5,"center")._1 as start_2,resize(start,end,5,"center")._2 as end_2 FROM ref LIMIT 1
      """.stripMargin
    assert(spark.sql(query).select("start_2").first().get(0) === 11870 && spark.sql(query).select("end_2").first().get(0) === 14411)
  }

  test( "Basic operation - resize - fix=start"){
    spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    spark.sqlContext.setConf("minOverlap","1")
    spark.sqlContext.setConf("maxGap","0")

    val query =
      """
        |SELECT chr,start,end,resize(start,end,5,"start")._1 as start_2,resize(start,end,5,"start")._2 as end_2 FROM ref LIMIT 1
      """.stripMargin
    assert(spark.sql(query).select("start_2").first().get(0) === 11873 && spark.sql(query).select("end_2").first().get(0) === 14414)
  }


  test( "Basic operation - resize - fix=end"){
    spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    spark.sqlContext.setConf("minOverlap","1")
    spark.sqlContext.setConf("maxGap","0")

    val query =
      """
        |SELECT chr,start,end,resize(start,end,5,"end")._1 as start_2,resize(start,end,5,"end")._2 as end_2 FROM ref LIMIT 1
      """.stripMargin
    assert(spark.sql(query).select("start_2").first().get(0) === 11868 && spark.sql(query).select("end_2").first().get(0) === 14409)
  }


}
