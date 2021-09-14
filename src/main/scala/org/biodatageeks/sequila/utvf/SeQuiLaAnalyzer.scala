package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.typeCoercionRules
import org.apache.spark.sql.{ResolveTableValuedFunctionsSeq, SparkSession}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.OptimizeUpdateFields
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.ResolveEncodersInScalaAgg
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.execution.command.CommandCheck
import org.apache.spark.sql.execution.datasources.v2.TableCapabilityCheck
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, FallBackFileSourceV2, FindDataSourceTable, HiveOnlyCheck, PreReadCheck, PreWriteCheck, PreprocessTableCreation, PreprocessTableInsertion, ResolveSQLOnFile}



class SeQuiLaAnalyzer(session: SparkSession) extends
  Analyzer( session.sessionState.analyzer.catalogManager){

  override val conf = session.sessionState.conf
  val catalog = session.sessionState.analyzer.catalogManager.v1SessionCatalog
  override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
    new FindDataSourceTable(session) +:
    new ResolveSQLOnFile(session) +:
    new FallBackFileSourceV2(session) +:
      new ResolveSessionCatalog(
        catalogManager, catalog.isTempView, catalog.isTempFunction) +:
    ResolveEncodersInScalaAgg+: session.extensions.buildResolutionRules(session)


  override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
    DetectAmbiguousSelfJoin +:
      PreprocessTableCreation(session) +:
      PreprocessTableInsertion +:
      DataSourceAnalysis +: session.extensions.buildPostHocResolutionRules(session)

  override val extendedCheckRules: Seq[LogicalPlan => Unit] =
    PreWriteCheck +:
      PreReadCheck +:
      HiveOnlyCheck +:
      TableCapabilityCheck +:
      CommandCheck +: session.extensions.buildCheckRules(session)


  private val v1SessionCatalog: SessionCatalog = catalogManager.v1SessionCatalog


  override def batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      // This rule optimizes `UpdateFields` expression chains so looks more like optimization rule.
      // However, when manipulating deeply nested schema, `UpdateFields` expression tree could be
      // very complex and make analysis impossible. Thus we need to optimize `UpdateFields` early
      // at the beginning of analysis.
      OptimizeUpdateFields,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions,
      SubstituteUnresolvedOrdinals),
    Batch("Disable Hints", Once,
      new ResolveHints.DisableHints),
    Batch("Hints", fixedPoint,
      ResolveHints.ResolveJoinStrategyHints,
      ResolveHints.ResolveCoalesceHints),
    Batch("Simple Sanity Check", Once,
      LookupFunctions),
    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctionsSeq ::
        ResolveNamespace(catalogManager) ::
        new ResolveCatalogs(catalogManager) ::
        ResolveUserSpecifiedColumns ::
        ResolveInsertInto ::
        ResolveRelations ::
        ResolveTables ::
        ResolvePartitionSpec ::
        AddMetadataColumns ::
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
        ResolveInlineTables ::
        ResolveHigherOrderFunctions(v1SessionCatalog) ::
        ResolveLambdaVariables ::
        ResolveTimeZone ::
        ResolveRandomSeed ::
        ResolveBinaryArithmetic ::
        ResolveUnion ::
        TypeCoercion.typeCoercionRules ++
          extendedResolutionRules : _*),
    Batch("Apply Char Padding", Once,
      ApplyCharTypePadding),
    Batch("Post-Hoc Resolution", Once,
      Seq(ResolveNoopDropTable) ++
        postHocResolutionRules: _*),
    Batch("Normalize Alter Table", Once, ResolveAlterTableChanges),
    Batch("Remove Unresolved Hints", Once,
      new ResolveHints.RemoveAllHints),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("UDF", Once,
      HandleNullInputsForUDF,
      ResolveEncodersInUDF),
    Batch("UpdateNullability", Once,
      UpdateAttributeNullability),
    Batch("Subquery", Once,
      UpdateOuterReferences),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )
}