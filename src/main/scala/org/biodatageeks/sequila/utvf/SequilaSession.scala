package org.apache.spark.sql


import org.apache.spark.sql.catalyst.analysis.{Analyzer, SeQuiLaAnalyzer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
//import org.apache.spark.sql.execution.command.{BAMCTASOptimizationRule, BAMIASOptimizationRule}
import org.apache.spark.sql.internal.SessionState
import org.biodatageeks.sequila.coverage.CoverageStrategy






case class SequilaSession(sparkSession: SparkSession) extends SparkSession(sparkSession.sparkContext) {
  @transient val sequilaAnalyzer = new SeQuiLaAnalyzer(sparkSession.sessionState.catalogManager,sparkSession.sessionState.conf)
  def executePlan(plan:LogicalPlan) =  new QueryExecution(sparkSession,sequilaAnalyzer.execute(plan))
  @transient override lazy val sessionState = SequilaSessionState(sparkSession,sequilaAnalyzer,executePlan)


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



object UTVFRegister {

  def main(args: Array[String]): Unit = {

    System.setSecurityManager(null)
    type ExtensionsBuilder = SparkSessionExtensions => Unit
   // val f: ExtensionsBuilder = { e => e.injectResolutionRule(ResolveTableValuedFunctionsSeq) }
    val spark = SparkSession.builder()
      .master("local[1]")
     // .withExtensions(f)
      .getOrCreate()
//
//    spark
//      .sql("select * from range(1,2)")
//        .explain(true

    //val context: SparkContext = new SparkContext(conf)
    val session: SparkSession = SequilaSession(spark)
    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil
  }

}
