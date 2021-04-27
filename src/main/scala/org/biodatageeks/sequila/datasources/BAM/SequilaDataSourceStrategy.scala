package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, EmptyRow, Expression, IntegerLiteral, Literal, NamedExpression, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalLimit, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy.selectFilters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.biodatageeks.sequila.datasources.BAM.BDGAlignmentRelation
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.utils.{Columns, InternalParams}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

import scala.collection.mutable.ArrayBuffer

case class SequilaDataSourceStrategy(spark: SparkSession) extends Strategy with Logging with CastSupport {

  def conf = spark.sqlContext.conf
  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {

    //optimized strategy for queries like SELECT (distinct )sampleId FROM  BDGAlignmentRelation
    case a: Aggregate if a.schema.length == 1 && a.schema.head.name == Columns.SAMPLE => {
      a.child match {
        case PhysicalOperation(projects, filters, l@LogicalRelation(t: PrunedFilteredScan, _, _,false)) => {
          l.catalogTable.get.provider match {
            case Some(InputDataType.BAMInputDataType) => {
              pruneFilterProject(
                l,
                projects,
                filters,
                (a, f) => toCatalystRDD(l, a, t.asInstanceOf[BDGAlignmentRelation[BAMBDGInputFormat]].buildScanSampleId)) :: Nil
            }
            case _ => Nil
          }
        }
        case _ => Nil
      }
    }

    case limit @ LocalLimit(limitValue @ IntegerLiteral(value), prj) => {
        limit.child  match {
          case PhysicalOperation(projects, filters, l@LogicalRelation(t: PrunedFilteredScan, _, _,false)) => {
            log.info("LIMIT clause detected, checking if can apply any optimizations...")
            t match {
              case value1: BDGAlignmentRelation[BAMBDGInputFormat] => {
                log.info("LIMIT clause detected and BAM relation and applying optimization")
                pruneFilterProject(
                  l,
                  projects,
                  filters,
                  (a, f) => toCatalystRDD(l, a,
                    value1.buildScanWithLimit(a.map(_.name).toArray, f, value))) :: Nil
              }
              case _ => Nil
            }
          }
          case _ => Nil
        }
    }

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: CatalystScan, _, _,false)) =>
      pruneFilterProjectRaw(
        l,
        projects,
        filters,
        (requestedColumns, allPredicates, _) =>
          toCatalystRDD(l, requestedColumns, t.buildScan(requestedColumns, allPredicates))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedFilteredScan, _, _,false)) => {
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil
    }

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedScan, _, _,false)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, _) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray))) :: Nil

    case l @ LogicalRelation(baseRelation: TableScan, _, _,false) =>
      RowDataSourceScanExec(
        l.output,
        l.output.indices,
        Set.empty,
        Set.empty,
        toCatalystRDD(l, baseRelation.buildScan()),
        baseRelation,
        None) :: Nil

    case _ => Nil
  }

  // Get the bucket ID based on the bucketing values.
  // Restriction: Bucket pruning works iff the bucketing column has one and only one column.
  def getBucketId(bucketColumn: Attribute, numBuckets: Int, value: Any): Int = {
    val mutableRow = new SpecificInternalRow(Seq(bucketColumn.dataType))
    mutableRow(0) = cast(Literal(value), bucketColumn.dataType).eval(null)
    val bucketIdGeneration = UnsafeProjection.create(
      HashPartitioning(bucketColumn :: Nil, numBuckets).partitionIdExpression :: Nil,
      bucketColumn :: Nil)

    bucketIdGeneration(mutableRow).getInt(0)
  }

  // Based on Public API.
  private def pruneFilterProject(
                                  relation: LogicalRelation,
                                  projects: Seq[NamedExpression],
                                  filterPredicates: Seq[Expression],
                                  scanBuilder: (Seq[Attribute], Array[Filter]) => RDD[InternalRow]) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      (requestedColumns, _, pushedFilters) => {
        scanBuilder(requestedColumns, pushedFilters.toArray)
      })
  }

  // Based on Catalyst expressions. The `scanBuilder` function accepts three arguments:
  //
  //  1. A `Seq[Attribute]`, containing all required column attributes. Used to handle relation
  //     traits that support column pruning (e.g. `PrunedScan` and `PrunedFilteredScan`).
  //
  //  2. A `Seq[Expression]`, containing all gathered Catalyst filter expressions, only used for
  //     `CatalystScan`.
  //
  //  3. A `Seq[Filter]`, containing all data source `Filter`s that are converted from (possibly a
  //     subset of) Catalyst filter expressions and can be handled by `relation`.  Used to handle
  //     relation traits (`CatalystScan` excluded) that support filter push-down (e.g.
  //     `PrunedFilteredScan` and `HadoopFsRelation`).
  //
  // Note that 2 and 3 shouldn't be used together.
  private def pruneFilterProjectRaw(
                                     relation: LogicalRelation,
                                     projects: Seq[NamedExpression],
                                     filterPredicates: Seq[Expression],
                                     scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter]) => RDD[InternalRow]): SparkPlan = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val candidatePredicates = filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}

    val (unhandledPredicates, pushedFilters, handledFilters) =
      selectFilters(relation.relation, candidatePredicates)

    // A set of column attributes that are only referenced by pushed down filters.  We can eliminate
    // them from requested columns.
    val handledSet = {
      val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
      val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
      AttributeSet(handledPredicates.flatMap(_.references)) --
        (projectSet ++ unhandledSet).map(relation.attributeMap)
    }

    // Combines all Catalyst filter `Expression`s that are either not convertible to data source
    // `Filter`s or cannot be handled by `relation`.
    val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)

    // These metadata values make scan plans uniquely identifiable for equality checking.
    // TODO(SPARK-17701) using strings for equality checking is brittle
    val metadata: Map[String, String] = {
      val pairs = ArrayBuffer.empty[(String, String)]

      // Mark filters which are handled by the underlying DataSource with an Astrisk
      if (pushedFilters.nonEmpty) {
        val markedFilters = for (filter <- pushedFilters) yield {
          if (handledFilters.contains(filter)) s"*$filter" else s"$filter"
        }
        pairs += ("PushedFilters" -> markedFilters.mkString("[", ", ", "]"))
      }
      pairs += ("ReadSchema" ->
        StructType.fromAttributes(projects.map(_.toAttribute)).catalogString)
      pairs.toMap
    }

    if (projects.map(_.toAttribute) == projects &&
      projectSet.size == projects.size &&
      filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns = projects
        // Safe due to if above.
        .asInstanceOf[Seq[Attribute]]
        // Match original case of attributes.
        .map(relation.attributeMap)
        // Don't request columns that are only referenced by pushed filters.
        .filterNot(handledSet.contains)

      val scan = RowDataSourceScanExec(
        projects.map(_.toAttribute),
        projects.map(_.toAttribute).indices,
        Set.empty,
        Set.empty,
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation,
        relation.catalogTable.map(_.identifier))
      filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan)
    } else {
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns =
        (projectSet ++ filterSet -- handledSet).map(relation.attributeMap).toSeq

      val scan = RowDataSourceScanExec(
        requestedColumns,
        requestedColumns.indices,
        Set.empty,
        Set.empty,
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation,
        relation.catalogTable.map(_.identifier))
      execution.ProjectExec(
        projects, filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
    }
  }

  /**
    * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
    */
  private[this] def toCatalystRDD(
                                   relation: LogicalRelation,
                                   output: Seq[Attribute],
                                   rdd: RDD[Row]): RDD[InternalRow] = {
    if (relation.relation.needConversion) {

      val toRow = RowEncoder(StructType.fromAttributes(output)).createSerializer()
      rdd.mapPartitions { iterator =>
        iterator.map(toRow)
      }
    } else {
      rdd.asInstanceOf[RDD[InternalRow]]
    }
  }

  /**
    * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
    */
  private[this] def toCatalystRDD(relation: LogicalRelation, rdd: RDD[Row]): RDD[InternalRow] = {
    toCatalystRDD(relation, relation.output, rdd)
  }
}

object SequilaDataSourceStrategy {
  /**
    * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
    *
    * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
    */
  protected[sql] def translateFilter(predicate: Expression): Option[Filter] = {
    predicate match {
      case expressions.EqualTo(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))
      case expressions.EqualTo(Literal(v, t), a: Attribute) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))

      case expressions.EqualNullSafe(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))
      case expressions.EqualNullSafe(Literal(v, t), a: Attribute) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))

      case expressions.GreaterThan(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))
      case expressions.GreaterThan(Literal(v, t), a: Attribute) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))

      case expressions.LessThan(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))
      case expressions.LessThan(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))

      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.LessThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.InSet(a: Attribute, set) =>
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, set.toArray.map(toScala)))

      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed
      // down.
      case expressions.In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(toScala)))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))
      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        // See SPARK-12218 for detailed discussion
        // It is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have (a = 2 AND trim(b) = 'blah') OR (c > 0)
        // and we do not understand how to convert trim(b) = 'blah'.
        // If we only convert a = 2, we will end up with
        // (a = 2) OR (c > 0), which will generate wrong results.
        // Pushing one leg of AND down is only safe to do at the top level.
        // You can see ParquetFilters' createFilter for more details.
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.And(leftFilter, rightFilter)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilter(child).map(sources.Not)

      case expressions.StartsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringStartsWith(a.name, v.toString))

      case expressions.EndsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringEndsWith(a.name, v.toString))

      case expressions.Contains(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringContains(a.name, v.toString))

      case _ => None
    }
  }

  /**
    * Selects Catalyst predicate [[Expression]]s which are convertible into data source [[Filter]]s
    * and can be handled by `relation`.
    *
    * @return A triplet of `Seq[Expression]`, `Seq[Filter]`, and `Seq[Filter]` . The first element
    *         contains all Catalyst predicate [[Expression]]s that are either not convertible or
    *         cannot be handled by `relation`. The second element contains all converted data source
    *         [[Filter]]s that will be pushed down to the data source. The third element contains
    *         all [[Filter]]s that are completely filtered at the DataSource.
    */
  protected[sql] def selectFilters(
                                    relation: BaseRelation,
                                    predicates: Seq[Expression]): (Seq[Expression], Seq[Filter], Set[Filter]) = {

    // For conciseness, all Catalyst filter expressions of type `expressions.Expression` below are
    // called `predicate`s, while all data source filters of type `sources.Filter` are simply called
    // `filter`s.

    // A map from original Catalyst expressions to corresponding translated data source filters.
    // If a predicate is not in this map, it means it cannot be pushed down.
    val translatedMap: Map[Expression, Filter] = predicates.flatMap { p =>
      translateFilter(p).map(f => p -> f)
    }.toMap

    val pushedFilters: Seq[Filter] = translatedMap.values.toSeq

    // Catalyst predicate expressions that cannot be converted to data source filters.
    val nonconvertiblePredicates = predicates.filterNot(translatedMap.contains)

    // Data source filters that cannot be handled by `relation`. An unhandled filter means
    // the data source cannot guarantee the rows returned can pass the filter.
    // As a result we must return it so Spark can plan an extra filter operator.
    val unhandledFilters = relation.unhandledFilters(translatedMap.values.toArray).toSet
    val unhandledPredicates = translatedMap.filter { case (p, f) =>
      unhandledFilters.contains(f)
    }.keys
    val handledFilters = pushedFilters.toSet -- unhandledFilters

    (nonconvertiblePredicates ++ unhandledPredicates, pushedFilters, handledFilters)
  }
}

