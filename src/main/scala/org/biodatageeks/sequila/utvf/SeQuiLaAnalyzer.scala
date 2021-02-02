package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.{ResolveTableValuedFunctionsSeq, SparkSession}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.ResolveEncodersInScalaAgg
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.execution.command.CommandCheck
import org.apache.spark.sql.execution.datasources.v2.TableCapabilityCheck
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, FallBackFileSourceV2, FindDataSourceTable, HiveOnlyCheck, PreReadCheck, PreWriteCheck, PreprocessTableCreation, PreprocessTableInsertion, ResolveSQLOnFile}



class SeQuiLaAnalyzer(session: SparkSession) extends
  Analyzer( session.sessionState.analyzer.catalogManager,
    session.sessionState.conf,
    session.sessionState.conf.optimizerMaxIterations){

  val conf = session.sessionState.conf
  val catalog = session.sessionState.catalog
  override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
    new FindDataSourceTable(session) +:
    new ResolveSQLOnFile(session) +:
    new FallBackFileSourceV2(session) +:
      new ResolveSessionCatalog(
        catalogManager, conf, catalog.isTempView, catalog.isTempFunction) +:
    ResolveEncodersInScalaAgg+: session.extensions.buildResolutionRules(session)


  override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
    new DetectAmbiguousSelfJoin(conf) +:
      PreprocessTableCreation(session) +:
      PreprocessTableInsertion(conf) +:
      DataSourceAnalysis(conf) +: session.extensions.buildPostHocResolutionRules(session)

  override val extendedCheckRules: Seq[LogicalPlan => Unit] =
    PreWriteCheck +:
      PreReadCheck +:
      HiveOnlyCheck +:
      TableCapabilityCheck +:
      CommandCheck(conf) +: session.extensions.buildCheckRules(session)


  private val v1SessionCatalog: SessionCatalog = catalogManager.v1SessionCatalog


  override lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions,
      new SubstituteUnresolvedOrdinals(conf)),
    Batch("Hints", fixedPoint,
      new ResolveHints.ResolveJoinStrategyHints(conf),
      new ResolveHints.ResolveCoalesceHints(conf)),
    Batch("Simple Sanity Check", Once,
      LookupFunctions),
    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctionsSeq ::
        ResolveNamespace(catalogManager) ::
        new ResolveCatalogs(catalogManager) ::
        ResolveInsertInto ::
        ResolveRelations ::
        ResolveTables ::
        ResolveReferences ::
        ResolveCreateNamedStruct ::
        ResolveDeserializer ::
        ResolveNewInstance ::
        ResolveUpCast ::
        ResolveGroupingAnalytics ::
        ResolvePivot ::
        ResolveOrdinalInOrderByAndGroupBy ::
        ResolveAggAliasInGroupBy ::
        ResolveMissingReferences ::
        ExtractGenerator ::
        ResolveGenerate ::
        ResolveFunctions ::
        ResolveAliases ::
        ResolveSubquery ::
        ResolveSubqueryColumnAliases ::
        ResolveWindowOrder ::
        ResolveWindowFrame ::
        ResolveNaturalAndUsingJoin ::
        ResolveOutputRelation ::
        ExtractWindowExpressions ::
        GlobalAggregates ::
        ResolveAggregateFunctions ::
        TimeWindowing ::
        ResolveInlineTables(conf) ::
        ResolveHigherOrderFunctions(v1SessionCatalog) ::
        ResolveLambdaVariables(conf) ::
        ResolveTimeZone(conf) ::
        ResolveRandomSeed ::
        ResolveBinaryArithmetic ::
        TypeCoercion.typeCoercionRules(conf) ++
          extendedResolutionRules : _*),
    Batch("Post-Hoc Resolution", Once, postHocResolutionRules: _*),
    Batch("Normalize Alter Table", Once, ResolveAlterTableChanges),
    Batch("Remove Unresolved Hints", Once,
      new ResolveHints.RemoveAllHints(conf)),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("UpdateNullability", Once,
      UpdateAttributeNullability),
    Batch("Subquery", Once,
      UpdateOuterReferences),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )
}