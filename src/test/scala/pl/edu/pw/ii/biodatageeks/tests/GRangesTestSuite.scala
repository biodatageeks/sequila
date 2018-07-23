package pl.edu.pw.ii.biodatageeks.tests

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{Row, SequilaSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.bdgenomics.utils.instrumentation.Metrics
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.rangejoins.methods.transformations.RangeMethods
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class GRangesTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{



  val schema = StructType(Seq(StructField("chr",StringType ),StructField("start",IntegerType ), StructField("end", IntegerType)))
  var ss: SequilaSession = _

  before{
    ss= new SequilaSession(spark)
    SequilaRegister.register(ss)
    UDFRegister.register(ss)
    System.setSecurityManager(null)
    //spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    Metrics.initialize(sc)
    val rdd1 = ss.sparkContext
      .textFile(getClass.getResource("/refFlat.txt.bz2").getPath)
      .map(r=>r.split('\t'))
      .map(r=>Row(
        (r(2).toString),
        r(4).toInt,
        r(5).toInt
      ))
    val ref =ss.sqlContext
      .createDataFrame(rdd1,schema)
    ref.createOrReplaceTempView("ref")

    val rdd2 = ss.sparkContext
      .textFile(getClass.getResource("/snp150Flagged.txt.bz2").getPath)
      .map(r=>r.split('\t'))
      .map(r=>Row(
        (r(1).toString),
        r(2).toInt,
        r(3).toInt
      ))
    val snp = ss.sqlContext
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
    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")
    assert(ss.sql(query).count === 616404L)
  }

  test("Basic operation - test overlap counts with min overlap-join condition"){
    //spark.sqlContext.udf.register("overlaplength", RangeMethods.calcOverlap _)
    //spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    val query =
      s"""
         |SELECT ref.*,snp.* FROM snp JOIN ref
         |ON (ref.chr=snp.chr
         |AND
         |snp.end>=ref.start
         |AND
         |snp.start<=ref.end
         |AND
         |overlaplength(snp.start,snp.end,ref.start,ref.end)>=10
         |)
         |
       """.stripMargin

    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")
    ss.sql(query).explain()
    assert(ss.sql(query).count === 7923L)
    ss.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
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

    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","10")
    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")
    assert(ss.sql(query).count === 7923L)
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

    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","10000")
    assert(ss.sql(query).count === 804488L)

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
    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")
    assert(ss.sql(query).count === 35941L)
  }

  test( "Basic operation - shift"){
      //spark.sqlContext.udf.register("shift", RangeMethods.shift _)
    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")

    val query =
      """
        |SELECT a.shiftedInterval.start as start_2, a.shiftedInterval.end as end_2
        |FROM (SELECT chr,start,end,shift(start,end,5) as shiftedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 11878 && ss.sql(query).select("end_2").first().get(0) === 14414)

  }

  test( "Basic operation - resize - fix=center"){
    //spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    ss.sqlContext.setConf("minOverlap","1")
    ss.sqlContext.setConf("maxGap","0")

    val query =
      """
        |SELECT a.resizedInterval.start as start_2, a.resizedInterval.end as end_2
        |FROM (SELECT chr,start,end,resize(start,end,5,"center") as resizedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 11870 && ss.sql(query).select("end_2").first().get(0) === 14411)
  }

  test( "Basic operation - resize - fix=start"){
    //spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    ss.sqlContext.setConf("minOverlap","1")
    ss.sqlContext.setConf("maxGap","0")

    val query =
      """
        |SELECT a.resizedInterval.start as start_2, a.resizedInterval.end as end_2
        |FROM (SELECT chr,start,end,resize(start,end,5,"start") as resizedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 11873 && ss.sql(query).select("end_2").first().get(0) === 14414)
  }


  test( "Basic operation - resize - fix=end"){
    //spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    ss.sqlContext.setConf("minOverlap","1")
    ss.sqlContext.setConf("maxGap","0")

    val query =
      """
        |SELECT a.resizedInterval.start as start_2, a.resizedInterval.end as end_2
        |FROM (SELECT chr,start,end,resize(start,end,5,"end") as resizedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 11868 && ss.sql(query).select("end_2").first().get(0) === 14409)
  }

  test( "Basic operation - flank - flankWidth positive, startFlank=true, both=false"){
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.start as start_2, a.flankedInterval.end as end_2
        |FROM (SELECT chr,start,end,flank(start,end,5,true,false) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 11868 && ss.sql(query).select("end_2").first().get(0) === 11872)
  }

  test( "Basic operation - flank - flankWidth positive, startFlank=false, both=false"){
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.start as start_2, a.flankedInterval.end as end_2
        |FROM (SELECT chr,start,end,flank(start,end,5,false,false) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 14410 && ss.sql(query).select("end_2").first().get(0) === 14414)
  }

  test( "Basic operation - flank - flankWidth positive, startFlank=true, both=true"){
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.start as start_2, a.flankedInterval.end as end_2
        |FROM (SELECT chr,start,end,flank(start,end,5,true,true) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 11868 && ss.sql(query).select("end_2").first().get(0) === 11877)
  }

  test( "Basic operation - flank - flankWidth positive, startFlank=false, both=true"){
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.start as start_2, a.flankedInterval.end as end_2
        |FROM (SELECT chr,start,end,flank(start,end,5,false,true) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 14405 && ss.sql(query).select("end_2").first().get(0) === 14414)
  }

  test( "Basic operation - flank - flankWidth negative, startFlank=true, both=false"){
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.start as start_2, a.flankedInterval.end as end_2
        |FROM (SELECT chr,start,end,flank(start,end,-5,true,false) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 11873 && ss.sql(query).select("end_2").first().get(0) === 11877)
  }

  test( "Basic operation - flank - flankWidth negative, startFlank=false, both=false"){
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.start as start_2, a.flankedInterval.end as end_2
        |FROM (SELECT chr,start,end,flank(start,end,-5,false,false) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 14405 && ss.sql(query).select("end_2").first().get(0) === 14409)
  }

  test( "Basic operation - flank - flankWidth negative, startFlank=true, both=true"){
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.start as start_2, a.flankedInterval.end as end_2
        |FROM (SELECT chr,start,end,flank(start,end,5,true,true) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 11868 && ss.sql(query).select("end_2").first().get(0) === 11877)
  }

  test( "Basic operation - flank - flankWidth negative, startFlank=false, both=true"){
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.start as start_2, a.flankedInterval.end as end_2
        |FROM (SELECT chr,start,end,flank(start,end,-5,false,true) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 14405 && ss.sql(query).select("end_2").first().get(0) === 14414)
  }

  test( "Basic operation - promoters"){
    //spark.sqlContext.udf.register("promoters", RangeMethods.promoters _)

    val query =
      s"""
        |SELECT a.promoterInterval.start as start_2, a.promoterInterval.end as end_2
        |FROM (SELECT chr, start, end, promoters(start,end,100,20) as promoterInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(ss.sql(query).select("start_2").first().get(0) === 11773 && ss.sql(query).select("end_2").first().get(0) === 11892)
  }

  test( "Basic operation - reflect"){
    //spark.sqlContext.udf.register("reflect", RangeMethods.reflect _)

    val query =
      s"""
        |SELECT a.reflectedInterval.start as start_2, a.reflectedInterval.end as end_2
        |FROM (SELECT chr, start, end, reflect(start,end,11000,15000) as reflectedInterval FROM ref LIMIT 1) a
      """.stripMargin

    assert(ss.sql(query).select("start_2").first().get(0) === 11591 && ss.sql(query).select("end_2").first().get(0) === 14127 )
  }
}
