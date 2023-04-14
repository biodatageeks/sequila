package org.biodatageeks.sequila.flagStat

import okhttp3.logging.HttpLoggingInterceptor.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, FlagStatTemplate, PileupTemplate, Row, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.biodatageeks.sequila.pileup.Pileup
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.conf.QualityConstants.{DEFAULT_BIN_SIZE, DEFAULT_MAX_QUAL}
import org.biodatageeks.sequila.utils.{FileFuncs, InternalParams, TableFuncs}
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, CRAMBDGInputFormat}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class FlagStatStrategy (spark:SparkSession) extends Strategy with Serializable {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case CreateDataSourceTableAsSelectCommand(table, mode, query, outputColumnNames) => {
        table.storage.locationUri match {
          case None => None
        }
        Nil
      }
      case InsertIntoHadoopFsRelationCommand(outputPath, staticPartitions, ifPartitionNotExists, partitionColumns, bucketSpec, fileFormat, options, query, mode, catalogTable, fileIndex, outputColumnNames) => {
        Nil
      }
      case FlagStatTemplate(tableNameOrPath, sampleId, output) => {
        val inputFormat = {
          if (sampleId != null)
            TableFuncs.getTableMetadata(spark, tableNameOrPath).provider
          else if (FileFuncs.getFileExtension(tableNameOrPath) == "bam") Some(InputDataType.BAMInputDataType)
          else None
        }
        inputFormat match {
          case Some(f) =>
            if (f == InputDataType.BAMInputDataType)
              FlagStatPlan[BAMBDGInputFormat](plan, spark, tableNameOrPath, sampleId, output) :: Nil
            else Nil
          case None => throw new RuntimeException("Only BAM file format is supported in flagStat function.")
        }
      }
      case _ => Nil
    }
  }
}

object FlagStatPlan extends Serializable {

}
case class FlagStatPlan [T<:BDGAlignInputFormat](plan:LogicalPlan, spark:SparkSession,
                                               tableNameOrPath:String,
                                               sampleId:String,
                                               output:Seq[Attribute] // ,directOrcWritePath: String = null)(implicit c: ClassTag[T]
                                               )
  extends SparkPlan with Serializable  with BDGAlignFileReaderWriter [T]{

  override protected def otherCopyArgs: Seq[AnyRef] = Seq()

  override def children: Seq[SparkPlan] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val fs = new FlagStat(spark);
    val rows = fs.handleFlagStat(tableNameOrPath, sampleId);
    val mapping = rows.collectAsMap;
    val fields = new ListBuffer[Long];
    FlagStat.Schema.fieldNames.foreach(x => {
      fields += mapping(x);
    })
    val result = InternalRow.fromSeq(fields);
    spark.sparkContext.parallelize(Seq(result));
  }
}
