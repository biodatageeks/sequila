package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}
import org.bdgenomics.adam.rdd.ADAMContext._
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.rangejoins.NCList.NCListsJoinStrategy
import org.biodatageeks.rangejoins.genApp.IntervalTreeJoinStrategy
import org.scalatest.{BeforeAndAfter, FunSuite}


class ADAMBenchmarkTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext {

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: " + (System.nanoTime - s) / 1e9 + " seconds")
    ret
  }
  val query = (
    s"""
       |SELECT * FROM snp JOIN ref
       |ON (ref.contigName=snp.contigName
       |AND
       |CAST(snp.end AS INTEGER)>=CAST(ref.start AS INTEGER)
       |AND
       |CAST(snp.start AS INTEGER)<=CAST(ref.end AS INTEGER)
       |)
       |
       """).stripMargin

  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))

  before {
    System.setSecurityManager(null)
    //spark.sparkContext.setLogLevel("INFO")
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (5*1024*1024).toString)
    val ref = spark.read.parquet(getClass.getResource("/refFlat.adam").getPath)
    ref.createOrReplaceTempView("ref")
    time(println(ref.count))

    val snp = spark.read.parquet(getClass.getResource("/snp150Flagged.adam").getPath)
    snp.createOrReplaceTempView("snp")
    time(println(snp.count))

    Metrics.initialize(sc)

    sc.addSparkListener(metricsListener)


  }

  test ("Join using bgd-spark-granges - broadcast"){

    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)

    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    time(assert(stageMetrics.runAndMeasure(spark.sqlContext.sql(query).count) === 616404L))

  }

  test ("Join using bgd-spark-granges - twophase"){
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
    sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (1024*1024).toString)
    time(assert(stageMetrics.runAndMeasure(spark.sqlContext.sql(query).count) === 616404L))
    val a = stageMetrics.createStageMetricsDF()
    val b= a
      .drop("jobId","stageId","name","submissionTime", "completionTime")
      .groupBy()
      .sum()


    b.select("sum(executorRunTime)","sum(executorCpuTime)","sum(shuffleTotalBytesRead)","sum(shuffleBytesWritten)")
      .show(100,false)
  }

  test ("Join using bgd-spark-granges NCList"){
    spark.experimental.extraStrategies = new NCListsJoinStrategy(spark) :: Nil
    time(assert(spark.sqlContext.sql(query).count === 616404L))
  }

  test ("Join using builtin spark algo"){

    spark.experimental.extraStrategies =  Nil
    time(assert(spark.sqlContext.sql(query).count === 616404L))
  }

  test ("Join using builtin genapp"){

    spark.experimental.extraStrategies =  new IntervalTreeJoinStrategy(spark) :: Nil
    time(assert(spark.sqlContext.sql(query).count === 616404L))
  }
//
//  test("Join using ADAM broadcast join"){
//
//    val featuresRef = sc.loadFeatures(getClass.getResource("/refFlat.adam").getPath)
//    val featuresSnp = sc.loadFeatures (getClass.getResource("/snp150Flagged.adam").getPath)
//    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
//    val res = featuresRef.broadcastRegionJoin(featuresSnp)
//    time(println(stageMetrics.runAndMeasure(res.rdd.count() ) ) )
//    //time(println(res.rdd.count()))
//  }
//
//  test("Join using ADAM shuffle join"){
//
//    val featuresRef = sc.loadFeatures(getClass.getResource("/refFlat.adam").getPath)
//    val featuresSnp = sc.loadFeatures (getClass.getResource("/snp150Flagged.adam").getPath)
//    val res = featuresRef.shuffleRegionJoin(featuresSnp)
//    //time(res.rdd.collect().size)
//    time(res.rdd.count())
//  }


  after{

    //Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    Metrics.stopRecording()

  }
}
