package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.typeCoercionRules
import org.apache.spark.sql.{ResolveTableValuedFunctionsSeq, SparkSession}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.OptimizeUpdateFields
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.aggregate.ResolveEncodersInScalaAgg
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.execution.command.CommandCheck
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, TableCapabilityCheck}
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, FallBackFileSourceV2, FindDataSourceTable, HiveOnlyCheck, PreReadCheck, PreWriteCheck, PreprocessTableCreation, PreprocessTableInsertion, ResolveSQLOnFile}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.collection.{Utils => CUtils}



class SeQuiLaAnalyzer(session: SparkSession) extends
  Analyzer( session.sessionState.analyzer.catalogManager) {

  override val conf = session.sessionState.conf
  val catalog = session.sessionState.analyzer.catalogManager.v1SessionCatalog
  override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
    new FindDataSourceTable(session) +:
      new ResolveSQLOnFile(session) +:
      new FallBackFileSourceV2(session) +:
      new ResolveSessionCatalog(
        catalogManager) +:
      ResolveEncodersInScalaAgg +: session.extensions.buildResolutionRules(session)



  override val extendedCheckRules: Seq[LogicalPlan => Unit] =
    PreWriteCheck +:
      PreReadCheck +:
      HiveOnlyCheck +:
      TableCapabilityCheck +:
      CommandCheck +: session.extensions.buildCheckRules(session)


  override def batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      // This rule optimizes `UpdateFields` expression chains so looks more like optimization rule.
      // However, when manipulating deeply nested schema, `UpdateFields` expression tree could be
      // very complex and make analysis impossible. Thus we need to optimize `UpdateFields` early
      // at the beginning of analysis.
      OptimizeUpdateFields,
      CTESubstitution,
      BindParameters,
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
    Batch("Keep Legacy Outputs", Once,
      KeepLegacyOutputs),
    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctionsSeq(catalog) ::
        new ResolveCatalogs(catalogManager) ::
        ResolveUserSpecifiedColumns ::
        ResolveInsertInto ::
        ResolveRelations ::
        ResolvePartitionSpec ::
        ResolveFieldNameAndPosition ::
        AddMetadataColumns ::
        DeduplicateRelations ::
        ResolveReferences ::
        ResolveLateralColumnAliasReference ::
        ResolveExpressionsWithNamePlaceholders ::
        ResolveDeserializer ::
        ResolveNewInstance ::
        ResolveUpCast ::
        ResolveGroupingAnalytics ::
        ResolvePivot ::
        ResolveUnpivot ::
        ResolveOrdinalInOrderByAndGroupBy ::
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
        SessionWindowing ::
        ResolveWindowTime ::
        ResolveDefaultColumns(ResolveRelations.resolveRelationOrTempView) ::
        ResolveInlineTables ::
        ResolveLambdaVariables ::
        ResolveTimeZone ::
        ResolveRandomSeed ::
        ResolveBinaryArithmetic ::
        ResolveUnion ::
        RewriteDeleteFromTable ::
        typeCoercionRules ++
          Seq(ResolveWithCTE) ++
          extendedResolutionRules: _*),
    Batch("Remove TempResolvedColumn", Once, RemoveTempResolvedColumn),
    Batch("Post-Hoc Resolution", Once,
      Seq(ResolveCommandsWithIfExists) ++
        postHocResolutionRules: _*),
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
      CleanupAliases),
    Batch("HandleSpecialCommand", Once,
      HandleSpecialCommand),
    Batch("Remove watermark for batch query", Once,
      EliminateEventTimeWatermark)
  )
}

