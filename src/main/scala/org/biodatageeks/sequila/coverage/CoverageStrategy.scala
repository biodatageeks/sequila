package org.biodatageeks.sequila.coverage

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.biodatageeks.sequila.coverage.CoverageMethodsMos.logger
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.pileup.PileupMethods.logger
import org.biodatageeks.sequila.utils.{InternalParams, TableFuncs}
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, CRAMBDGInputFormat}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


class CoverageStrategy(spark: SparkSession) extends Strategy with Serializable  {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case BDGCoverage(tableName,sampleId,result,target,output) =>
      val inputFormat = TableFuncs
        .getTableMetadata(spark, tableName)
        .provider
      inputFormat match {
        case Some(f) =>
          if (f == InputDataType.BAMInputDataType)
            BDGCoveragePlan[BAMBDGInputFormat](plan, spark, tableName, sampleId,result,target, output) :: Nil
          else if (f == InputDataType.CRAMInputDataType)
            BDGCoveragePlan[CRAMBDGInputFormat](plan, spark, tableName, sampleId, result,target, output) :: Nil
          else Nil
        case None => throw new RuntimeException("Only BAM and CRAM file formats are supported in pileup function.")
      }

    case _ => Nil
  }

}

case class UpdateStruct(
                         upd:mutable.HashMap[(String,Int),(Option[Array[Short]],Short)],
                         shrink:mutable.HashMap[(String,Int), Int],
                         minmax:mutable.HashMap[String,(Int,Int)]
                       )



case class BDGCoveragePlan [T<:BDGAlignInputFormat](plan: LogicalPlan, spark: SparkSession,
                                                    table:String, sampleId:String, result:String, target:Option[String],
                                                    output: Seq[Attribute])(implicit c: ClassTag[T])
  extends SparkPlan with Serializable  with BDGAlignFileReaderWriter [T]{


  def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {


    spark
      .sparkContext
      .getPersistentRDDs
      .filter(t=> t._2.name==InternalParams.RDDEventsName)
      .foreach(_._2.unpersist())

    val schema = plan.schema
    val sampleTable = TableFuncs
      .getTableMetadata(spark,table)

    val fileExtension = sampleTable.provider match{
      case Some(f) =>
        if (f == InputDataType.BAMInputDataType) "bam"
        else if (f == InputDataType.CRAMInputDataType) "cram"
        else throw new Exception("Only BAM and CRAM file formats are supported in bdg_coverage.")
      case None => throw new Exception("Wrong file extension - only BAM and CRAM file formats are supported in bdg_coverage.")
    }
    val samplePath = (sampleTable
      .location.toString
      .split('/')
      .dropRight(1) ++ Array(s"$sampleId*.$fileExtension"))
      .mkString("/")

    val refPath = sqlContext
      .sparkContext
      .hadoopConfiguration
      .get(CRAMBDGInputFormat.REFERENCE_SOURCE_PATH_PROPERTY)
    val logger =  Logger.getLogger(this.getClass.getCanonicalName)
      logger.info(s"Processing $samplePath with reference: $refPath")
    lazy val alignments = readBAMFile(spark.sqlContext, samplePath, if( refPath == null || refPath.length == 0) None else Some(refPath))

    val filterFlag = spark.conf.get(InternalParams.filterReadsByFlag, "1796").toInt

    lazy val events = CoverageMethodsMos.readsToEventsArray(alignments,filterFlag)

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
          val loggerIN =  Logger.getLogger(this.getClass.getCanonicalName)
          if (loggerIN.isDebugEnabled()) loggerIN.debug(s"#### CoverageAccumulator Adding partition record for: chr:=$contigName,start=$minPos,end=$maxPos,span=${maxPos - minPos + 1}, max cigar length: $maxCigarLength")

          acc.add(cu)
        }
      }
    events.setName(InternalParams.RDDEventsName)

    def prepareBroadcast(a: CovUpdate) = {

      if (logger.isDebugEnabled()) logger.debug("Preparing broadcast")

      val contigRanges = a.left
      val updateArray = a.right
      val updateMap = new mutable.HashMap[(String, Int), (Option[Array[Short]], Short)]()
      val shrinkMap = new mutable.HashMap[(String, Int), Int]()
      val minmax = new mutable.HashMap[String, (Int, Int)]()
      var it = 0
      for (c<-contigRanges.sortBy(r=>(r.contig,r.minPos))) {
          val contig = c.contig
          if (!minmax.contains(contig))
            minmax += contig -> (Int.MaxValue, 0)
          val filterUpd =  updateArray
            .filter(f => (f.contig == contig && f.startPoint + f.cov.length > c.minPos) && f.minPos < c.minPos)
            .sortBy(r => (r.contig, r.minPos)) // updates for overlaps
          val upd = filterUpd //should be always 1 or 0 elements, not true for long reads
        val cumSum = updateArray //cumSum of all contigRanges lt current contigRange
          .filter(f => f.contig == c.contig && f.minPos < c.minPos)
          .map(_.cumSum)
          .sum

          if(upd.nonEmpty){ // if there are any updates from overlapping partitions
              for(u <- upd) {
                val overlapLength =
                  if ((u.startPoint + u.cov.length) > c.maxPos &&  ( (contigRanges.length - 1 == it) ||  contigRanges(it+1).contig != c.contig))  {
                    u.startPoint + u.cov.length - c.minPos + 1
                  }
                  else if ((u.startPoint + u.cov.length) > c.maxPos) {
                    c.maxPos - c.minPos
                  }//if it's the last part in contig or the last at all
                  else {
                    u.startPoint + u.cov.length - c.minPos  + 1
                  }

                if (logger.isDebugEnabled()) logger.debug(s"##### Overlap length $overlapLength for $it from ${u.contig},${u.minPos}, ${u.startPoint},${u.cov.length}")
                shrinkMap.get((u.contig, u.minPos)) match {
                  case Some(s) => shrinkMap.update((u.contig, u.minPos), math.min(s,c.minPos - u.minPos + 1))
                  case _ =>  shrinkMap += (u.contig, u.minPos) -> (c.minPos - u.minPos + 1)
                }
                updateMap.get((c.contig, c.minPos)) match {
                  case Some(up) =>
                    val arr = Array.fill[Short](math.max(0,u.startPoint-c.minPos))(0) ++ u.cov.takeRight(overlapLength)
                    val newArr=up._1.get.zipAll(arr, 0.toShort ,0.toShort).map{ case (x, y) => (x + y).toShort }
                    val newCumSum= (up._2 - u.cov.takeRight(overlapLength).sum).toShort

                    if (logger.isDebugEnabled()) logger.debug(s"overlap u.minPos=${u.minPos} len = ${newArr.length}")
                    if (logger.isDebugEnabled())  logger.debug(s"next update to updateMap: c.minPos=${c.minPos}, u.minPos=${u.minPos} curr_len${up._1.get.length}, new_len = ${newArr.length}")

                    if(u.minPos < c.minPos)
                      updateMap.update((c.contig, c.minPos), (Some(newArr), newCumSum))
                    else
                      updateMap.update((c.contig, u.minPos), (Some(newArr), newCumSum)) // delete anything that is > c.minPos
                  case _ =>
                    if (logger.isDebugEnabled()) logger.debug(s"overlap u.minPos=${u.minPos} u.max = ${u.minPos + overlapLength - 1} len = $overlapLength")
                    if (logger.isDebugEnabled()) logger.debug(s"first update to updateMap: ${math.max(0,u.startPoint-c.minPos)}, $overlapLength ")
                    updateMap += (c.contig, c.minPos) -> (Some(Array.fill[Short](math.max(0,u.startPoint-c.minPos))(0) ++u.cov.takeRight(overlapLength)), (cumSum - u.cov.takeRight(overlapLength).sum).toShort)
                }
              }
            if (logger.isDebugEnabled()) logger.debug(s"#### Update struct length: ${updateMap(c.contig, c.minPos)._1.get.length}")
            if (logger.isDebugEnabled()) logger.debug(s"#### Shrinking struct ${shrinkMap.mkString("|")}")
            }
            else {
              updateMap += (c.contig, c.minPos) -> (None, cumSum)

          }
          if (c.minPos < minmax(contig)._1)
            minmax(contig) = (c.minPos, minmax(contig)._2)
          if (c.maxPos > minmax(contig)._2)
            minmax(contig) = (minmax(contig)._1, c.maxPos)
        it += 1
      }
      if (logger.isDebugEnabled()) logger.debug(s"Prepared broadcast $updateMap, $shrinkMap")

      UpdateStruct(updateMap, shrinkMap, minmax)
    }

    val covBroad = spark.sparkContext.broadcast(prepareBroadcast(acc.value()))
    lazy val reducedEvents = CoverageMethodsMos.updateContigRange(covBroad, events)

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
    val allPos = spark.sqlContext.getConf(InternalParams.ShowAllPositions, "false").toBoolean

    //check if it's a window length or a table name
    val maybeWindowLength =
      try {
              target match {
                case Some(t) => Some(t.toInt)
                case _ => None
              }
      } catch {
        case _: Exception => None
    }


    lazy val cov =
      if(maybeWindowLength.isDefined) //fixed-length window
        CoverageMethodsMos.eventsToCoverage(sampleId, reducedEvents, covBroad.value.minmax, blocksResult, allPos,maybeWindowLength,None)

          .keyBy(_.key)
          .reduceByKey((a,b) =>
            CovRecordWindow(a.contig,
              a.start,
              a.end,
              (a.asInstanceOf[CovRecordWindow].overLap.get * a.asInstanceOf[CovRecordWindow].cov + b.asInstanceOf[CovRecordWindow].overLap.get * b.asInstanceOf[CovRecordWindow].cov )/
                (a.asInstanceOf[CovRecordWindow].overLap.get + b.asInstanceOf[CovRecordWindow].overLap.get),
              Some(a.asInstanceOf[CovRecordWindow].overLap.get + b.asInstanceOf[CovRecordWindow].overLap.get) ) )
          .map(_._2)

       else
        CoverageMethodsMos.eventsToCoverage(sampleId, reducedEvents, covBroad.value.minmax, blocksResult, allPos,None, None)

    if(maybeWindowLength.isDefined) {   // windows

      cov.mapPartitions(p => {
        val proj = UnsafeProjection.create(schema)
        p.map(r => proj.apply(InternalRow.fromSeq(Seq(
          UTF8String.fromString(r.contig), r.start, r.end, r.asInstanceOf[CovRecordWindow].cov))))
      })
    } else { // regular blocks
      cov.mapPartitions(p => {
        val proj = UnsafeProjection.create(schema)
        p.map(r => proj.apply(InternalRow.fromSeq(Seq(/*UTF8String.fromString(sampleId),*/
          UTF8String.fromString(r.contig), r.start, r.end, r.asInstanceOf[CovRecord].cov))))
      })
    }

  }

  def convertBlocksToBases(blocks: RDD[CovRecord]): RDD[CovRecord] = {
    blocks
      .flatMap {
        r => {
          val chr = r.contig
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

