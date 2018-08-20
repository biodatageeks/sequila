package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Subquery, SubqueryAlias}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand, RunnableCommand}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.datasources.BAM.{BAMDataSource, BDGAlignFileReaderWriter}
import org.biodatageeks.datasources.BDGInputDataType
import org.biodatageeks.inputformats.BDGAlignInputFormat
import org.biodatageeks.utils.{BDGInternalParams, BDGTableFuncs}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

import scala.reflect.ClassTag

//class BAMCTASStrategy(spark: SparkSession) extends Strategy with Serializable {
//  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
//      case CreateDataSourceTableAsSelectCommand(table, mode, query) => {
//        val srcTable = query.collect { case r: SubqueryAlias => r.alias }.head
//        val filters = query.collect { case f: Filter => f.condition.simpleString }
//        filters.foreach(println(_))
//        val srcTableMeta = BDGTableFuncs
//          .getTableMetadata(spark, srcTable)
//        srcTableMeta.provider match {
//          case Some(f) => {
//            if (f == BDGInputDataType.BAMInputDataType) {
//              val srcTableDir = BDGTableFuncs.getTableDirectory(spark, srcTable)
//              val output = srcTableMeta
//                .schema
//                .toAttributes
//              BAMCTASPlan[BAMBDGInputFormat](plan, spark,table, srcTableDir,output) :: Nil
//              //CreateDataSourceTableAsSelectCommand(table, mode, query)
//            }
//            else Nil
//
//          }
//          case _ => Nil
//        }
//      }
//
//      case _ => Nil
//
//    }
//
//}


//case class BAMCTASPlan[T<:BDGAlignInputFormat](plan: LogicalPlan, @transient spark: SparkSession, table:CatalogTable,srcTableDir:String, output:Seq[Attribute])(implicit c: ClassTag[T])
//  extends SparkPlan with Serializable  with BDGAlignFileReader [T]{
//
//  override def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
//
//  //first saveFiles
//
//  //finally create table
////      println("Creating...")
////    val query = (
////          s"""
////             | CREATE TABLE ${table.qualifiedName} USING ${table.provider.get}
////
////             |OPTIONS(path "${table.location}")
////        """.stripMargin)
//
//    lazy val ds = new  CreateDataSourceTableCommand(table,false)
//    ds.run(spark)
//
//
//    spark
//    .sparkContext
//    .emptyRDD[InternalRow]
//
//
//  }
//
//  def children: Seq[SparkPlan] = Nil
//
//}
