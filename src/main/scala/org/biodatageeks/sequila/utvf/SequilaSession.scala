package org.apache.spark.sql


import org.apache.spark.sql.catalyst.analysis.{Analyzer, SeQuiLaAnalyzer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
//import org.apache.spark.sql.execution.command.{BAMCTASOptimizationRule, BAMIASOptimizationRule}
import org.apache.spark.sql.internal.SessionState
import org.biodatageeks.sequila.coverage.CoverageStrategy






case class SequilaSession(sparkSession: SparkSession) extends SparkSession(sparkSession.sparkContext) {
  @transient val analyzer = new SeQuiLaAnalyzer(
      sparkSession)
  def executePlan(plan:LogicalPlan) =  new QueryExecution(sparkSession,analyzer.execute(plan))
  @transient override lazy val sessionState = SequilaSessionState(sparkSession,analyzer,executePlan)


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
