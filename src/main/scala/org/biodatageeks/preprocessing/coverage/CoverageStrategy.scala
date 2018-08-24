package org.biodatageeks.preprocessing.coverage

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.AccumulatorV2
import org.biodatageeks.datasources.BAM.{BDGAlignFileReaderWriter, BDGSAMRecord}
import org.biodatageeks.datasources.BDGInputDataType
import org.biodatageeks.inputformats.BDGAlignInputFormat
import org.biodatageeks.preprocessing.coverage.CoverageReadFunctions._
import org.biodatageeks.utils.{BDGInternalParams, BDGTableFuncs}
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, BAMInputFormat, CRAMBDGInputFormat, SAMRecordWritable}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


class CoverageStrategy(spark: SparkSession) extends Strategy with Serializable  {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case Coverage(tableName,output) => CoveragePlan(plan,spark,tableName,output) :: Nil
    case CoverageHist(tableName,output) => CoverageHistPlan(plan,spark,tableName,output) :: Nil

    //add support for CRAM

    case BDGCoverage(tableName,sampleId,method,result,output) => {
      val inputFormat = BDGTableFuncs
        .getTableMetadata(spark, tableName)
        .provider
      inputFormat match {
        case Some(f) => {

          if (f == BDGInputDataType.BAMInputDataType)
            BDGCoveragePlan[BAMBDGInputFormat](plan, spark, tableName, sampleId, method, result, output) :: Nil
          else if (f == BDGInputDataType.CRAMInputDataType)
            BDGCoveragePlan[CRAMBDGInputFormat](plan, spark, tableName, sampleId, method, result, output) :: Nil
          else Nil
        }
        case None => throw new Exception("Only BAM and CRAM file formats are supported in bdg_coverage.")
      }
    }

    case _ => Nil
  }

}

case class CoveragePlan(plan: LogicalPlan, spark: SparkSession, table:String, output: Seq[Attribute]) extends SparkPlan with Serializable {
  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    import spark.implicits._
    val ds = spark.sql(s"select * FROM ${table}")
      .as[BDGSAMRecord]
      .filter(r=>r.contigName != null)
    val schema = plan.schema
    val cov = ds.rdd.baseCoverage(None,Some(4),sorted=false)
      cov
      .mapPartitions(p=>{
       val proj =  UnsafeProjection.create(schema)
       p.map(r=>   proj.apply(InternalRow.fromSeq(Seq(UTF8String.fromString(r.sampleId),
          UTF8String.fromString(r.chr),r.position,r.coverage))))
      })
  }


  def children: Seq[SparkPlan] = Nil
}

case class CoverageHistPlan(plan: LogicalPlan, spark: SparkSession, table:String, output: Seq[Attribute])
  extends SparkPlan with Serializable {

  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
    import spark.implicits._
    val ds = spark.sql(s"select * FROM ${table}")
      .as[BDGSAMRecord]
      .filter(r=>r.contigName != null)
    val schema = plan.schema
    val params = CoverageHistParam(CoverageHistType.MAPQ,Array(0,1,2,3,50))
    val cov = ds.rdd.baseCoverageHist(Some(0),None,params)
//    val emptyIntArray =
//      ExpressionEncoder[Array[Int]]().resolveAndBind().toRow(Array.emptyIntArray).getArray(0)
    cov
      .mapPartitions(p=>{
        val proj =  UnsafeProjection.create(schema)
        val exprEnc =  ExpressionEncoder[Array[Int]]().resolveAndBind()
        p.map(r=>   proj.apply(InternalRow.fromSeq(
          Seq(
            UTF8String.fromString(r.sampleId),
            UTF8String.fromString(r.chr),
            r.position,
           exprEnc.toRow(r.coverage).getArray(0),
            //UTF8String.fromString(r.coverage.mkString(",") ),
            r.coverageTotal) ) ) )
      })
    //spark.sparkContext.emptyRDD[InternalRow]
  }
  def children: Seq[SparkPlan] = Nil
}



case class UpdateStruct(
                         upd:mutable.HashMap[(String,Int),(Option[Array[Short]],Short)],
                         shrink:mutable.HashMap[(String,Int),(Int)],
                         minmax:mutable.HashMap[String,(Int,Int)]
                       )



case class BDGCoveragePlan [T<:BDGAlignInputFormat](plan: LogicalPlan, spark: SparkSession,
                                                    table:String, sampleId:String, method: String, result:String, output: Seq[Attribute])(implicit c: ClassTag[T])
  extends SparkPlan with Serializable  with BDGAlignFileReaderWriter [T]{


  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {

    spark
      .sparkContext
      .getPersistentRDDs
        .foreach(_._2.unpersist()) //FIXME: add filtering not all RDDs
    val schema = plan.schema
    val sampleTable = BDGTableFuncs
      .getTableMetadata(spark,table)
    val fileExtension = sampleTable.provider match{
      case Some(f) => {
        if (f == BDGInputDataType.BAMInputDataType) "bam"
        else if (f == BDGInputDataType.CRAMInputDataType) "cram"
        else throw new Exception("Only BAM and CRAM file formats are supported in bdg_coverage.")
      }
      case None => throw new Exception("Wrong file extension - only BAM and CRAM file formats are supported in bdg_coverage.")
    }
    val samplePath = (sampleTable
      .location.toString
      .split('/')
      .dropRight(1) ++ Array(s"${sampleId}*.${fileExtension}"))
      .mkString("/")

    setLocalConf(spark.sqlContext)
    lazy val alignments = readBAMFile(spark.sqlContext,samplePath)

    lazy val events = CoverageMethodsMos.readsToEventsArray(alignments)

    val covUpdate = new CovUpdate(new ArrayBuffer[RightCovEdge](), new ArrayBuffer[ContigRange]())
    val acc = new CoverageAccumulatorV2(covUpdate)
    spark
      .sparkContext
      .register(acc, "CoverageAcc")
    events
      .persist(StorageLevel.MEMORY_AND_DISK)
      .foreach {
        c => {
          val maxCigarLength = c._2._5
          val covToUpdate = c._2._1.takeRight(maxCigarLength)
          val minPos = c._2._2
          val maxPos = c._2._3
          val startPoint = maxPos - maxCigarLength
          val contigName = c._1
          val right = RightCovEdge(contigName, minPos, startPoint, covToUpdate, c._2._1.sum)
          val left = ContigRange(contigName, minPos, maxPos)
          val cu = new CovUpdate(ArrayBuffer(right), ArrayBuffer(left))
          acc.add(cu)
        }
      }

    def prepareBroadcast(a: CovUpdate) = {
      val contigRanges = a.left
      val updateArray = a.right
      val updateMap = new mutable.HashMap[(String, Int), (Option[Array[Short]], Short)]()
      val shrinkMap = new mutable.HashMap[(String, Int), (Int)]()
      val minmax = new mutable.HashMap[String, (Int, Int)]()

      contigRanges.foreach {
        c =>
          val contig = c.contigName
          if (!minmax.contains(contig))
            minmax += contig -> (Int.MaxValue, 0)

          val upd = updateArray
            .filter(f => (f.contigName == c.contigName && f.startPoint + f.cov.length > c.minPos) && f.minPos < c.minPos)
            .headOption //should be always 1 or 0 elements
        val cumSum = updateArray //cumSum of all contigRanges lt current contigRange
          .filter(f => f.contigName == c.contigName && f.minPos < c.minPos)
          .map(_.cumSum)
          .sum
          upd match {
            case Some(u) => {
              val overlapLength = (u.startPoint + u.cov.length) - c.minPos + 1
              shrinkMap += (u.contigName, u.minPos) -> (c.minPos - u.minPos + 1)
              updateMap += (c.contigName, c.minPos) -> (Some(u.cov.takeRight(overlapLength)), (cumSum - u.cov.takeRight(overlapLength).sum).toShort)
            }
            case None => {
              updateMap += (c.contigName, c.minPos) -> (None, cumSum)

            }
          }
          if (c.minPos < minmax(contig)._1)
            minmax(contig) = (c.minPos, minmax(contig)._2)
          if (c.maxPos > minmax(contig)._2)
            minmax(contig) = (minmax(contig)._1, c.maxPos)
      }

      UpdateStruct(updateMap, shrinkMap, minmax)
    }

    val covBroad = spark.sparkContext.broadcast(prepareBroadcast(acc.value()))

    lazy val reducedEvents = CoverageMethodsMos.upateContigRange(covBroad, events)

    val blocksResult = {
      result.toLowerCase() match {
        case "blocks" | "" | null =>
          true
        case "bases" =>
          false
        case _ =>
          throw new Exception ("Unsupported parameter for coverage calculation")
      }
    }
    val allPos = spark.sqlContext.getConf(BDGInternalParams.ShowAllPositions, "false").toBoolean

    lazy val cov = CoverageMethodsMos.eventsToCoverage(sampleId, reducedEvents, covBroad.value.minmax, blocksResult, allPos)

    cov.mapPartitions(p => {
      val proj = UnsafeProjection.create(schema)
      p.map(r => proj.apply(InternalRow.fromSeq(Seq(/*UTF8String.fromString(sampleId),*/
        UTF8String.fromString(r.contigName), r.start, r.end, r.cov))))
    })

  }

  def convertBlocksToBases(blocks: RDD[CovRecord]): RDD[CovRecord] = {
    blocks
      .flatMap {
        r => {
          val chr = r.contigName
          val start = r.start
          val end = r.end
          val cov = r.cov

          val array = new Array[CovRecord](end - start + 1)

          var cnt = 0
          var position = start

          while (position <= end) {
            array(cnt) = CovRecord(chr, position, position, cov)
            cnt += 1
            position += 1
          }
          array
        }
      }
  }

  def children: Seq[SparkPlan] = Nil
}

