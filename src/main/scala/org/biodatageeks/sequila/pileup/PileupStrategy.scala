package org.biodatageeks.sequila.pileup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{PileupTemplate, SparkSession, Strategy}
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.pileup.conf.QualityConstants.{DEFAULT_MAX_QUAL,DEFAULT_BIN_SIZE}
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.utils.{InternalParams, TableFuncs}
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, CRAMBDGInputFormat}

import scala.reflect.ClassTag

class PileupStrategy (spark:SparkSession) extends Strategy with Serializable {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case PileupTemplate(tableName, sampleId, refPath, qual, binSize, output) =>
        val inputFormat = TableFuncs.getTableMetadata(spark, tableName).provider
        inputFormat match {
          case Some(f) =>
            if (f == InputDataType.BAMInputDataType)
              PileupPlan[BAMBDGInputFormat](plan, spark, tableName, sampleId, refPath, qual, binSize, output) :: Nil
            else if (f == InputDataType.CRAMInputDataType)
              PileupPlan[CRAMBDGInputFormat](plan, spark, tableName, sampleId, refPath, qual, binSize, output) :: Nil
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
                                               qual: Boolean,
                                               binSize: Option[Int],
                                               output:Seq[Attribute])(implicit c: ClassTag[T])
  extends SparkPlan with Serializable  with BDGAlignFileReaderWriter [T]{

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(c)

  override def children: Seq[SparkPlan] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val conf = setupPileupConfiguration()
    new Pileup(spark).handlePileup(tableName, sampleId, refPath, output, conf)
  }


  private def setupPileupConfiguration(): Conf = {
    val conf = new Conf
    if (refPath.isEmpty) {// FIXME -> change to Option
      conf.coverageOnly = true
      return conf
    }
    val maxQual = spark.conf.get(InternalParams.maxBaseQualityValue, DEFAULT_MAX_QUAL.toString).toInt
    conf.maxQuality = maxQual
    conf.maxQualityIndex = maxQual + 1
    conf.includeBaseQualities = qual
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
