package org.apache.spark.sql.execution.command

import java.net.URI
import java.util.concurrent.Callable

import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils, UnresolvedCatalogRelation}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, InsertableRelation}
import org.biodatageeks.datasources.BAM.BDGAlignmentRelation
import org.biodatageeks.datasources.BDGInputDataType
import org.biodatageeks.utils.{BDGInternalParams, BDGTableFuncs}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

case class CreateBAMDataSourceTableAsSelectCommand(
                                                 table: CatalogTable,
                                                 mode: SaveMode,
                                                 query: LogicalPlan)
  extends RunnableCommand {

  override protected def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableName has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"Table $tableName already exists. You need to drop it first.")
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      saveDataIntoTable(
        sparkSession, table, table.storage.locationUri, query, SaveMode.Append, tableExists = true)
    } else {
      assert(table.schema.isEmpty)

      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
        Some(sessionState.catalog.defaultTablePath(table.identifier))
      } else {
        table.storage.locationUri
      }
      val result = saveDataIntoTable(
        sparkSession, table, tableLocation, query, SaveMode.Overwrite, tableExists = false)
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        // We will use the schema of resolved.relation as the schema of the table (instead of
        // the schema of df). It is important since the nullability may be changed by the relation
        // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
        schema = result.schema)
      sessionState.catalog.createTable(newTable, ignoreIfExists = false)

      result match {
        case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
          sparkSession.sqlContext.conf.manageFilesourcePartitions =>
          // Need to recover partitions into the metastore so our saved data is visible.
          sessionState.executePlan(AlterTableRecoverPartitionsCommand(table.identifier)).toRdd
        case _ =>
      }
    }
    Seq.empty[Row]
  }
  def saveDataIntoTable(
                         session: SparkSession,
                         table: CatalogTable,
                         tableLocation: Option[URI],
                         data: LogicalPlan,
                         mode: SaveMode,
                         tableExists: Boolean): BaseRelation = {
    // Create the relation based on the input logical plan: `data`.
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val filters = query.collect { case f: Filter => f.condition.toString().replace("'","") }.headOption
    val limit = query.collect { case f:GlobalLimit => f.children(0).toString().split('\n')(0).stripPrefix("'").replace("LocalLimit","").trim}.headOption
    val srcTable = query.collect { case f:SubqueryAlias => f.alias }.head

    val dataSource = DataSource(
      session,
      className = table.provider.get,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption
        ++ Map(BDGInternalParams.BAMCTASDir->BDGTableFuncs.getTableDirectory(session,srcTable),
        BDGInternalParams.BAMCTASFilter -> filters.getOrElse(""), BDGInternalParams.BAMCTASLimit -> limit.getOrElse("")) ,
      catalogTable = if (tableExists) Some(table) else None)

    //filters.foreach(println(_))
    try {
      dataSource.writeAndRead(mode, Dataset.ofRows(session, query))
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table ${table.identifier.unquotedString}", ex)
        throw ex
    }
  }
}


class BAMCTASOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {


  private def readDataSourceTable(table: CatalogTable): LogicalPlan = {
    val qualifiedTableName = QualifiedTableName(table.database, table.identifier.table)
    val catalog = spark.sessionState.catalog
    catalog.getCachedPlan(qualifiedTableName, new Callable[LogicalPlan]() {
      override def call(): LogicalPlan = {
        val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
        val dataSource =
          DataSource(
            spark,
            // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
            // inferred at runtime. We should still support it.
            userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
            partitionColumns = table.partitionColumnNames,
            bucketSpec = table.bucketSpec,
            className = table.provider.get,
            options = table.storage.properties ++ pathOption,
            catalogTable = Some(table))

        LogicalRelation(dataSource.resolveRelation(checkFilesExist = false), table)
      }
    })
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CreateTable(table, mode, Some(query))  if table.provider.getOrElse("NA") == BDGInputDataType.BAMInputDataType  => {
      CreateBAMDataSourceTableAsSelectCommand(table, mode, query)
    }
    case UnresolvedCatalogRelation(tableMeta) if DDLUtils.isDatasourceTable(tableMeta) => readDataSourceTable(tableMeta)
  }

}

case class InsertIntoBAMDataSourceCommand(
                                        logicalRelation: LogicalRelation,
                                        query: LogicalPlan,
                                        overwrite: Boolean,
                                        srcTable: String )
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val relation = logicalRelation.relation.asInstanceOf[BDGAlignmentRelation[BAMBDGInputFormat]]
    val data = Dataset.ofRows(sparkSession, query)
    // Data has been casted to the target relation's schema by the PreprocessTableInsertion rule.
    //relation.insert(data, overwrite)
    relation.insertWithHeader(data, overwrite,srcTable)

    // Re-cache all cached plans(including this relation itself, if it's cached) that refer to this
    // data source relation.
    sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, logicalRelation)

    Seq.empty[Row]
  }
}

class BAMIASOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case InsertIntoTable(u @ UnresolvedRelation(_), parts, query, overwrite,ifPartitionNotExists ) if
    BDGTableFuncs.getTableMetadata(spark,u.tableName).provider.get == BDGInputDataType.BAMInputDataType
    => {
        val srcTable = query.collect { case r: SubqueryAlias => r.alias }.head
        val meta = BDGTableFuncs.getTableMetadata(spark,u.tableName)
        val rel = new BDGAlignmentRelation(meta.location.getPath)(spark.sqlContext)
        val logicalRelation = LogicalRelation(rel, meta.schema.toAttributes, Some(meta))
        InsertIntoBAMDataSourceCommand(logicalRelation, query, overwrite, srcTable)
      }
    }


}

//if l.catalogTable.get.provider.getOrElse("NA") == BDGInputDataType.BAMInputDataType
//case InsertIntoTable(l @ LogicalRelation(_: InsertableRelation, _, _),
//parts, query, overwrite,ifPartitionNotExists ) if l.catalogTable.get.provider.getOrElse("NA") == BDGInputDataType.BAMInputDataType  =>