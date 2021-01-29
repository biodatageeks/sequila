package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.ResolveTableValuedFunctionsSeq
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf

import scala.util.Random


class SeQuiLaAnalyzer(catalog: CatalogManager, conf: SQLConf) extends Analyzer(catalog, conf, conf.optimizerMaxIterations){
  //override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Seq(ResolveTableValuedFunctionsSeq)


  //  override lazy val batches: Seq[Batch] = Seq(
  //    Batch("Custeom", fixedPoint, ResolveTableValuedFunctionsSeq),
  //    Batch("Hints", fixedPoint, new ResolveHints.ResolveBroadcastHints(conf),
  //      ResolveHints.RemoveAllHints))


  private val v1SessionCatalog: SessionCatalog = catalogManager.v1SessionCatalog

  var sequilaOptmazationRules: Seq[Rule[LogicalPlan]] = Nil

//  override lazy val batches: Seq[Batch] = Seq(
//    Batch("Hints", fixedPoint,
//      new ResolveHints.ResolveBroadcastHints(conf),
//      ResolveHints.RemoveAllHints),
//    Batch("Simple Sanity Check", Once,
//      LookupFunctions),
//    Batch("Substitution", fixedPoint,
//      CTESubstitution,
//      WindowsSubstitution,
//      EliminateUnions,
//      new SubstituteUnresolvedOrdinals(conf)),
//    Batch("Resolution", fixedPoint,
//      ResolveTableValuedFunctionsSeq ::
//      ResolveRelations ::
//        ResolveReferences ::
//        ResolveCreateNamedStruct ::
//        ResolveDeserializer ::
//        ResolveNewInstance ::
//        ResolveUpCast ::
//        ResolveGroupingAnalytics ::
//        ResolvePivot ::
//        ResolveOrdinalInOrderByAndGroupBy ::
//        ResolveAggAliasInGroupBy ::
//        ResolveMissingReferences ::
//        ExtractGenerator ::
//        ResolveGenerate ::
//        ResolveFunctions ::
//        ResolveAliases ::
//        ResolveSubquery ::
//        ResolveSubqueryColumnAliases ::
//        ResolveWindowOrder ::
//        ResolveWindowFrame ::
//        ResolveNaturalAndUsingJoin ::
//
//        ExtractWindowExpressions ::
//        GlobalAggregates ::
//        ResolveAggregateFunctions ::
//        TimeWindowing ::
//        ResolveInlineTables(conf) ::
//        ResolveTimeZone(conf) ::
//        TypeCoercion.typeCoercionRules(conf) ++
//          extendedResolutionRules : _*),
//    Batch("Post-Hoc Resolution", Once, postHocResolutionRules: _*),
//    Batch("SeQuiLa", Once,sequilaOptmazationRules: _*), //SeQuilaOptimization rules
//    Batch("View", Once,
//      AliasViewChild(conf)),
//    Batch("Nondeterministic", Once,
//      PullOutNondeterministic),
//    Batch("UDF", Once,
//      HandleNullInputsForUDF),
//    Batch("FixNullability", Once,
//      FixNullability),
//    Batch("Subquery", Once,
//      UpdateOuterReferences),
//    Batch("Cleanup", fixedPoint,
//      CleanupAliases)
//  )



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
    Batch("SeQuiLa", Once,sequilaOptmazationRules: _*), //SeQuilaOptimization rules
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