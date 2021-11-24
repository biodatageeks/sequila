package org.apache.spark.sql


import org.apache.spark.sql.catalyst.analysis.{Analyzer, SeQuiLaAnalyzer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.{ArrayType, ByteType, MapType, ShortType}


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
    * @return pileup as Dataset[Row]
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
    sparkSession.sessionState.udfRegistration,
    () => sparkSession.sessionState.catalog,
    sparkSession.sessionState.sqlParser,
    () =>customAnalyzer,
    () =>sparkSession.sessionState.optimizer,
    sparkSession.sessionState.planner,
    () => sparkSession.sessionState.streamingQueryManager,
    sparkSession.sessionState.listenerManager,
    () =>sparkSession.sessionState.resourceLoader,
    executePlan,
    (sparkSession:SparkSession,sessionState: SessionState) => sessionState.clone(sparkSession),
    sparkSession.sessionState.columnarRules,
    sparkSession.sessionState.queryStagePrepRules
  ){
}
