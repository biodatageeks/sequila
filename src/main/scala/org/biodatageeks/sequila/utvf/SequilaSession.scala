package org.apache.spark.sql


import htsjdk.samtools.ValidationStringency
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.analysis.{Analyzer, SeQuiLaAnalyzer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{CommandExecutionMode, QueryExecution}
import org.apache.spark.sql.execution.datasources.SequilaDataSourceStrategy
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.{ArrayType, ByteType, MapType, ShortType}
import org.biodatageeks.sequila.pileup.PileupStrategy
import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.biodatageeks.sequila.utils.{InternalParams, UDFRegister}
import org.biodatageeks.sequila.utvf.GenomicIntervalStrategy



object SequilaSession {

  val logger = Logger.getLogger(this.getClass.getCanonicalName)
  def apply(spark: SparkSession): SequilaSession = {
    val ss = new SequilaSession(spark)
    register(ss)
    ss
  }

  def register(spark : SparkSession) = {
    logger.info("Registering SeQuiLa extensions")
    spark.experimental.extraStrategies =
      Seq(
        new SequilaDataSourceStrategy(spark),
        new IntervalTreeJoinStrategyOptim(spark),
        new PileupStrategy(spark),
        new GenomicIntervalStrategy(spark)

      )
    /*Set params*/
    spark
      .sparkContext
      .hadoopConfiguration
      .setInt("mapred.max.split.size", spark.sqlContext.getConf(InternalParams.InputSplitSize,"134217728").toInt)

    spark
      .sqlContext
      .setConf(InternalParams.IOReadAlignmentMethod,"hadoopBAM")

    spark
      .sqlContext
      .setConf(InternalParams.BAMValidationStringency, ValidationStringency.SILENT.toString)

    spark
      .sqlContext
      .setConf(InternalParams.UseIntelGKL, "true")

    spark
      .sqlContext
      .setConf(InternalParams.useVectorizedOrcWriter, "false")

    spark
      .sqlContext
      .setConf(InternalParams.EnableInstrumentation, "false")

    UDFRegister.register(spark)
  }

}


case class SequilaSession(sparkSession: SparkSession) extends SparkSession(sparkSession.sparkContext) {
  @transient val analyzer = new SeQuiLaAnalyzer(
      sparkSession)
  def executePlan(plan:LogicalPlan) =  new QueryExecution(sparkSession,analyzer.execute(plan))
  @transient override lazy val sessionState = SequilaSessionState(sparkSession,analyzer,executePlan)

  import sparkSession.implicits._

  /**
    * Calculate depth of coverage in a block format
    *  +------+---------+-------+---+--------+
    *  |contig|pos_start|pos_end|ref|coverage|
    *  +------+---------+-------+---+--------+
    *
    * @param path BAM/CRAM file (with an index file)
    * @param refPath Reference file (with an index file)
    * @return coverage as Dataset[Coverage]
    */
  def coverage(path: String, refPath: String) : Dataset[Coverage] ={

    new Dataset(sparkSession, PileupTemplate(path, refPath, false, false), Encoders.kryo[Row]).as[Coverage]
  }

  /**
    * Calculate pileup in block/base format
    * +------+---------+-------+------------------+--------+--------+-----------+---------+--------------------+
    *  |contig|pos_start|pos_end|               ref|coverage|countRef|countNonRef|     alts|               quals|
    *  +------+---------+-------+------------------+--------+--------+-----------+---------+--------------------+
    *
    * @param path BAM/CRAM file (with an index file)
    * @param refPath Reference file (with an index file)
    * @param quals Include/exclude quality map in case of alts (default: true )
    * @return pileup as Dataset[Pileup]
    */
  def pileup(path: String, refPath: String, quals: Boolean = true) : Dataset[Pileup] ={
    if(!quals) {
      new Dataset(sparkSession, PileupTemplate(path, refPath, true, quals), Encoders.kryo[Row])
      .withColumn("quals", typedLit[Option[Map[Byte, Array[Short]]]](None) )
    }
    else
      new Dataset(sparkSession, PileupTemplate(path, refPath, true, quals), Encoders.kryo[Row])
  }.as[Pileup]

}


case class SequilaSessionState(sparkSession: SparkSession, customAnalyzer: Analyzer, executePlan: LogicalPlan => QueryExecution)
  extends SessionState(
    sparkSession.sharedState,
    sparkSession.sessionState.conf,
    sparkSession.sessionState.experimentalMethods,
    sparkSession.sessionState.functionRegistry,
    sparkSession.sessionState.tableFunctionRegistry,
    sparkSession.sessionState.udfRegistration,
    () => sparkSession.sessionState.catalog,
    sparkSession.sessionState.sqlParser,
    () =>customAnalyzer,
    () =>sparkSession.sessionState.optimizer,
    sparkSession.sessionState.planner,
    () => sparkSession.sessionState.streamingQueryManager,
    sparkSession.sessionState.listenerManager,
    () =>sparkSession.sessionState.resourceLoader,
    sparkSession.sessionState.executePlan,
    (sparkSession:SparkSession,sessionState: SessionState) => sessionState.clone(sparkSession),
    sparkSession.sessionState.columnarRules,
    sparkSession.sessionState.queryStagePrepRules
  ){
}
