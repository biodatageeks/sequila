package org.biodatageeks.sequila.tests.rangejoins

import java.io.{OutputStreamWriter, PrintWriter}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{Row, SequilaSession}
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.utils.{Columns, InternalParams, Interval, UDFRegister}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter}

class GRangesTestSuite
    extends AnyFunSuite
    with DataFrameSuiteBase
    with BeforeAndAfter
    with SharedSparkContext
    with Interval {

  var ss: SequilaSession = _
  before {
    ss = SequilaSession(spark)
    UDFRegister.register(ss)
    System.setSecurityManager(null)
    val rdd1 = ss.sparkContext
      .textFile(getClass.getResource("/refFlat.txt.bz2").getPath)
      .map(r => r.split('\t'))
      .map(
        r =>
          Row(
            r(2).toString,
            r(4).toInt,
            r(5).toInt
        ))
    val ref = ss.sqlContext
      .createDataFrame(rdd1, schema)
    ref.createOrReplaceTempView("ref")

    val rdd2 = ss.sparkContext
      .textFile(getClass.getResource("/snp150Flagged.txt.bz2").getPath)
      .map(r => r.split('\t'))
      .map(
        r =>
          Row(
            r(1).toString,
            r(2).toInt,
            r(3).toInt
        ))
    val snp = ss.sqlContext
      .createDataFrame(rdd2, schema)
    snp.createOrReplaceTempView("snp")

  }

  test("Basic operation - test raw counts") {
    val queryRef =
      """
        |SELECT * FROM ref
      """.stripMargin

    val querySnp =
      """
        |SELECT * FROM snp
      """.stripMargin

    assert(spark.sql(queryRef).count() === 74734L)

    assert(spark.sql(querySnp).count() === 203915L)

  }

  test("Basic operation - test overlap counts") {
    val query =
      s"""
         |SELECT snp.*,ref.*
         |FROM snp JOIN ref
         |ON (ref.${Columns.CONTIG}=snp.${Columns.CONTIG}
         |AND snp.${Columns.END}>=ref.${Columns.START}
         |AND snp.${Columns.START}<=ref.${Columns.END}
         |)
         |
       """.stripMargin

    ss.sqlContext.setConf(InternalParams.minOverlap, "1")
    ss.sqlContext.setConf(InternalParams.maxGap, "0")
    assert(ss.sql(query).count === 616404L)
  }

  test("Basic operation - test overlap counts with min overlap-join condition") {

    val query =
      s"""
         |SELECT ref.*,snp.*
         |FROM snp JOIN ref
         |ON (ref.${Columns.CONTIG}=snp.${Columns.CONTIG}
         |AND snp.${Columns.END}>=ref.${Columns.START}
         |AND snp.${Columns.START}<=ref.${Columns.END}
         |AND overlaplength(snp.${Columns.START},snp.${Columns.END},ref.${Columns.START},ref.${Columns.END})>=10
         |)
         |
       """.stripMargin

    ss.sqlContext.setConf(InternalParams.maxGap, "0")
    ss.sql(query).explain()
    assert(ss.sql(query).count === 7923L)
    ss.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
  }

  test("Basic operation - test overlap counts with min overlap") {
    val query =
      s"""
         |SELECT ref.*,snp.*
         |FROM snp JOIN ref
         |ON (ref.${Columns.CONTIG}=snp.${Columns.CONTIG}
         |AND snp.${Columns.END}>=ref.${Columns.START}
         |AND snp.${Columns.START}<=ref.${Columns.END}
         |)
       """.stripMargin

    ss.sqlContext.setConf(InternalParams.minOverlap, "10")
    ss.sqlContext.setConf(InternalParams.maxGap, "0")
    assert(ss.sql(query).count === 7923L)
  }

  test("Basic operation - test overlap counts with max gap") {

    val query =
      s"""
         |SELECT ref.*, snp.${Columns.CONTIG} as chr2, snp.${Columns.START} as start2, snp.${Columns.END} as end2
         |FROM snp JOIN ref ON (ref.${Columns.CONTIG}=snp.${Columns.CONTIG}
         |AND snp.${Columns.END}>=ref.${Columns.START}
         |AND snp.${Columns.START}<=ref.${Columns.END}
         |)
         |
       """.stripMargin

    ss.sqlContext.setConf(InternalParams.minOverlap, "1")
    ss.sqlContext.setConf(InternalParams.maxGap, "10000")
    assert(ss.sql(query).count === 804488L)

  }

  test("Basic operation - subsetByOverlaps") {

    val query =
      s"""
         |SELECT * FROM ref a,
         |(SELECT distinct ref.* FROM ref JOIN snp
         |ON (ref.${Columns.CONTIG}=snp.${Columns.CONTIG}
         |AND snp.${Columns.END}>=ref.${Columns.START}
         |AND snp.${Columns.START}<=ref.${Columns.END}
         |) ) b
         |WHERE a.${Columns.CONTIG} = b.${Columns.CONTIG} AND a.${Columns.START}=b.${Columns.START} AND a.${Columns.END} = b.${Columns.END}
       """.stripMargin
    ss.sqlContext.setConf(InternalParams.minOverlap, "1")
    ss.sqlContext.setConf(InternalParams.maxGap, "0")
    assert(ss.sql(query).count === 35941L)
  }

  test("Basic operation - shift") {
    //spark.sqlContext.udf.register("shift", RangeMethods.shift _)
    ss.sqlContext.setConf(InternalParams.minOverlap, "1")
    ss.sqlContext.setConf(InternalParams.maxGap, "0")

    val query =
      s"""
        |SELECT a.shiftedInterval.${Columns.START} as start_2, a.shiftedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END},shift(${Columns.START},${Columns.END},5) as shiftedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 11878 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 14414)

  }

  test("Basic operation - resize - fix=center") {
    //spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    ss.sqlContext.setConf("minOverlap", "1")
    ss.sqlContext.setConf("maxGap", "0")

    val query =
      s"""
         |SELECT a.resizedInterval.${Columns.START} as start_2, a.resizedInterval.${Columns.END} as end_2
         |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END}, resize(${Columns.START},${Columns.END},5,"center") as resizedInterval FROM ref LIMIT 1) a
            """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 11870 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 14411)
  }

  test("Basic operation - resize - fix=start") {
    //spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    ss.sqlContext.setConf("minOverlap", "1")
    ss.sqlContext.setConf("maxGap", "0")

    val query =
      s"""
          |SELECT a.resizedInterval.${Columns.START} as start_2, a.resizedInterval.${Columns.END} as end_2
          |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END}, resize(${Columns.START},${Columns.END},5,"start") as resizedInterval FROM ref LIMIT 1) a
            """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 11873 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 14414)
  }

  test("Basic operation - resize - fix=end") {
    //spark.sqlContext.udf.register("resize", RangeMethods.resize _)
    ss.sqlContext.setConf("minOverlap", "1")
    ss.sqlContext.setConf("maxGap", "0")

    val query =
      s"""
        |SELECT a.resizedInterval.${Columns.START} as start_2, a.resizedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END}, resize(${Columns.START},${Columns.END},5,"end") as resizedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 11868 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 14409)
  }

  test(
    "Basic operation - flank - flankWidth positive, startFlank=true, both=false") {
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
         |SELECT a.flankedInterval.${Columns.START} as start_2, a.flankedInterval.${Columns.END} as end_2
         |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END}, flank(${Columns.START},${Columns.END},5,true,false) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 11868 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 11872)
  }

  test(
    "Basic operation - flank - flankWidth positive, startFlank=false, both=false") {
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.${Columns.START} as start_2, a.flankedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END}, flank(${Columns.START},${Columns.END},5,false,false) as flankedInterval FROM ref LIMIT 1) a
        """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 14410 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 14414)
  }

  test(
    "Basic operation - flank - flankWidth positive, startFlank=true, both=true") {
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.${Columns.START} as start_2, a.flankedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END}, flank(${Columns.START},${Columns.END},5,true,true) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 11868 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 11877)
  }

  test(
    "Basic operation - flank - flankWidth positive, startFlank=false, both=true") {
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.${Columns.START} as start_2, a.flankedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END}, flank(${Columns.START},${Columns.END},5,false,true) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 14405 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 14414)
  }

  test(
    "Basic operation - flank - flankWidth negative, startFlank=true, both=false") {
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.${Columns.START} as start_2, a.flankedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END},flank(${Columns.START},${Columns.END},-5,true,false) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 11873 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 11877)
  }

  test(
    "Basic operation - flank - flankWidth negative, startFlank=false, both=false") {
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.${Columns.START} as start_2, a.flankedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END},flank(${Columns.START},${Columns.END},-5,false,false) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 14405 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 14409)
  }

  test(
    "Basic operation - flank - flankWidth negative, startFlank=true, both=true") {
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.${Columns.START} as start_2, a.flankedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END},flank(${Columns.START},${Columns.END},5,true,true) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 11868 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 11877)
  }

  test(
    "Basic operation - flank - flankWidth negative, startFlank=false, both=true") {
    //spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT a.flankedInterval.${Columns.START} as start_2, a.flankedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG},${Columns.START},${Columns.END},flank(${Columns.START},${Columns.END},-5,false,true) as flankedInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 14405 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 14414)
  }

  test("Basic operation - promoters") {
    //spark.sqlContext.udf.register("promoters", RangeMethods.promoters _)

    val query =
      s"""
        |SELECT a.promoterInterval.${Columns.START} as start_2, a.promoterInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, promoters(${Columns.START},${Columns.END},100,20) as promoterInterval FROM ref LIMIT 1) a
      """.stripMargin
    assert(
      ss.sql(query).select("start_2").first().get(0) === 11773 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 11892)
  }

  test("Basic operation - reflect") {
    //spark.sqlContext.udf.register("reflect", RangeMethods.reflect _)

    val query =
      s"""
        |SELECT a.reflectedInterval.${Columns.START} as start_2, a.reflectedInterval.${Columns.END} as end_2
        |FROM (SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, reflect(${Columns.START},${Columns.END},11000,15000) as reflectedInterval FROM ref LIMIT 1) a
      """.stripMargin

    assert(
      ss.sql(query).select("start_2").first().get(0) === 11591 && ss
        .sql(query)
        .select("end_2")
        .first()
        .get(0) === 14127)
  }

}
