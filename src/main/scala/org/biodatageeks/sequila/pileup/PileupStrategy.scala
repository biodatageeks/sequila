package org.biodatageeks.sequila.pileup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.{PileupTemplate, SparkSession, Strategy}
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.pileup.conf.QualityConstants.{DEFAULT_BIN_SIZE, DEFAULT_MAX_QUAL}
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.utils.{InternalParams, TableFuncs}
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, CRAMBDGInputFormat}

import scala.reflect.ClassTag

class PileupStrategy (spark:SparkSession) extends Strategy with Serializable {

  var vectorizedOrcWritePath: String = null
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case CreateDataSourceTableAsSelectCommand(table, mode, query, outputColumnNames) => {
         table.storage.locationUri match {
           case Some(path) => vectorizedOrcWritePath = path.getPath
           case None => None
         }
        Nil
      }
      case InsertIntoHadoopFsRelationCommand(outputPath, staticPartitions, ifPartitionNotExists, partitionColumns, bucketSpec, fileFormat, options, query, mode, catalogTable, fileIndex, outputColumnNames) => {
        vectorizedOrcWritePath = outputPath.toString
        Nil
      }
      case PileupTemplate(tableName, sampleId, refPath, alts, quals, binSize, output) =>
        val inputFormat = TableFuncs.getTableMetadata(spark, tableName).provider
        inputFormat match {
          case Some(f) =>
            if (f == InputDataType.BAMInputDataType)
              PileupPlan[BAMBDGInputFormat](plan, spark, tableName, sampleId,
                refPath, alts, quals, binSize, output,vectorizedOrcWritePath) :: Nil
            else if (f == InputDataType.CRAMInputDataType)
              PileupPlan[CRAMBDGInputFormat](plan, spark, tableName, sampleId,
                refPath, alts, quals, binSize, output,  vectorizedOrcWritePath) :: Nil
            else Nil
          case None => throw new RuntimeException("Only BAM and CRAM file formats are supported in pileup function.")
        }
      case _ => Nil
    }
  }
}

object PileupPlan extends Serializable {

}
case class PileupPlan [T<:BDGAlignInputFormat](plan:LogicalPlan, spark:SparkSession,
                                               tableName:String,
                                               sampleId:String,
                                               refPath: String,
                                               alts: Boolean,
                                               quals: Boolean,
                                               binSize: Option[Int],
                                               output:Seq[Attribute],
                                               directOrcWritePath: String = null)(implicit c: ClassTag[T])
  extends SparkPlan with Serializable  with BDGAlignFileReaderWriter [T]{

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(c)

  override def children: Seq[SparkPlan] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val conf = setupPileupConfiguration(spark)
    new Pileup(spark).handlePileup(tableName, sampleId, refPath, output, conf)
  }


  private def setupPileupConfiguration(spark: SparkSession): Conf = {
    val conf = new Conf
    val isLocal = spark.sparkContext.isLocal
    conf.useVectorizedOrcWriter =  spark.sqlContext.getConf(InternalParams.useVectorizedOrcWriter, "false") match {
      case t: String if t.toLowerCase() == "true" && isLocal => true //FIXME: vectorized Writer supported only in local mode
      case _ => false
    }

    if(conf.useVectorizedOrcWriter && directOrcWritePath != null) {
      val orcCompressCodec = spark.conf.get("spark.sql.orc.compression.codec")
      conf.orcCompressCodec = orcCompressCodec
      conf.vectorizedOrcWriterPath = directOrcWritePath
    }

    if (!alts && !quals ) {// FIXME -> change to Option
      conf.coverageOnly = true
      conf.outputFieldsNum = output.size
      return conf
    }
    val maxQual = spark.conf.get(InternalParams.maxBaseQualityValue, DEFAULT_MAX_QUAL.toString).toInt
    conf.maxQuality = maxQual
    conf.maxQualityIndex = maxQual + 1
    conf.includeBaseQualities = quals
    conf.outputFieldsNum = output.size
    if(binSize.isDefined) {
      conf.isBinningEnabled = true
      conf.binSize = binSize.get
      conf.qualityArrayLength = Math.round(conf.maxQuality  / conf.binSize.toDouble).toInt + 1
    } else {
      conf.isBinningEnabled = false
      conf.binSize = DEFAULT_BIN_SIZE
      conf.qualityArrayLength = conf.maxQuality + 1
    }
    conf
  }

}
