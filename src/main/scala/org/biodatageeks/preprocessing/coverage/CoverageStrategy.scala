package org.biodatageeks.preprocessing.coverage

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.biodatageeks.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.datasources.BDGInputDataType
import org.biodatageeks.inputformats.BDGAlignInputFormat
import org.biodatageeks.utils.{BDGInternalParams, BDGTableFuncs}
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, CRAMBDGInputFormat}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


class CoverageStrategy(spark: SparkSession) extends Strategy with Serializable  {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    //add support for CRAM

    case BDGCoverage(tableName,sampleId,result,target,output) => {
      val inputFormat = BDGTableFuncs
        .getTableMetadata(spark, tableName)
        .provider
      inputFormat match {
        case Some(f) => {

          if (f == BDGInputDataType.BAMInputDataType)
            BDGCoveragePlan[BAMBDGInputFormat](plan, spark, tableName, sampleId,result,target, output) :: Nil
          else if (f == BDGInputDataType.CRAMInputDataType)
            BDGCoveragePlan[CRAMBDGInputFormat](plan, spark, tableName, sampleId, result,target, output) :: Nil
          else Nil
        }
        case None => throw new Exception("Only BAM and CRAM file formats are supported in bdg_coverage.")
      }
    }

    case _ => Nil
  }

}

case class UpdateStruct(
                         upd:mutable.HashMap[(String,Int),(Option[Array[Short]],Short)],
                         shrink:mutable.HashMap[(String,Int),(Int)],
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
      .filter((t)=> t._2.name==BDGInternalParams.RDDEventsName)
      .foreach(_._2.unpersist())

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

    val refPath = sqlContext
      .sparkContext
      .hadoopConfiguration
      .get(CRAMBDGInputFormat.REFERENCE_SOURCE_PATH_PROPERTY)
    val logger =  Logger.getLogger(this.getClass.getCanonicalName)
      logger.info(s"Processing ${samplePath} with reference: ${refPath}")
    lazy val alignments = readBAMFile(spark.sqlContext, samplePath, if( refPath == null || refPath.length == 0) None else Some(refPath))

    val filterFlag = spark.sqlContext.getConf(BDGInternalParams.filterReadsByFlag, "1796").toInt

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
          loggerIN.debug(s"#### CoverageAccumulator Adding partition record for: chr:=${contigName},start=${minPos},end=${maxPos},span=${maxPos - minPos + 1}, max cigar length: $maxCigarLength")
          acc.add(cu)
        }
      }
    events.setName(BDGInternalParams.RDDEventsName)

    def prepareBroadcast(a: CovUpdate) = {

      logger.debug("Preparing broadcast")

      val contigRanges = a.left
      val updateArray = a.right
      val updateMap = new mutable.HashMap[(String, Int), (Option[Array[Short]], Short)]()
      val shrinkMap = new mutable.HashMap[(String, Int), (Int)]()
      val minmax = new mutable.HashMap[String, (Int, Int)]()
//      val updateArraySplit = new ArrayBuffer[RightCovEdge]()

    //In case of long reads overlap from n partition can overlap not only n+1 partition but any number of forthcoming partitions
      //
//      for(u<-updateArray){
//        val partitions = contigRanges.filter(c=> (u.contigName == c.contigName && u.startPoint + u.cov.length > c.minPos) && u.minPos < c.minPos)
//        if (partitions.length  == 1)
//          updateArraySplit.append(u)
//        else if(partitions.length > 1){
//          partitions
//        }
//      }
      var it = 0
      for (c<-contigRanges.sortBy(r=>(r.contigName,r.minPos))) {
          val contig = c.contigName
          if (!minmax.contains(contig))
            minmax += contig -> (Int.MaxValue, 0)
          val filterUpd =  updateArray
              .sortBy(r=>(r.contigName,r.minPos))
            .filter(f => (f.contigName == contig && f.startPoint + f.cov.length > c.minPos) && f.minPos < c.minPos) // updates for overlaps
        //.filter(f => (f.contigName == c.contigName && f.startPoint + f.cov.length > c.minPos) && f.minPos < c.minPos)
          val upd = filterUpd //should be always 1 or 0 elements, not true for long reads
         // logger.warn(s"#### Partittion ${c.contigName},${c.minPos},${c.maxPos} overlaped by : ${if(filterUpd.length>0) {filterUpd.mkString("|")} else "0"} update structs")

        //filterUpd.foreach(u=>logger.warn(s"UpdateStructs overlapping ${c.contigName},${c.minPos},${c.maxPos} : ${u.contigName},${u.minPos}, ${u.startPoint}, ${u.cov.length} "))

        val cumSum = updateArray //cumSum of all contigRanges lt current contigRange
          .filter(f => f.contigName == c.contigName && f.minPos < c.minPos)
          .map(_.cumSum)
          .sum

          if(upd.length > 0){ // if there are any updates from overlapping partitions
              for(u <- upd) {
                val overlapLength =
                  if ((u.startPoint + u.cov.length) > c.maxPos &&  ( (contigRanges.length - 1 == it) ||  contigRanges(it+1).contigName != c.contigName))  {
//                  logger.warn(s"Counting overlap #1 ${u.startPoint + u.cov.length - c.minPos + 1}")
                    u.startPoint + u.cov.length - c.minPos + 1
                  }
                  else if ((u.startPoint + u.cov.length) > c.maxPos) {
                    (c.maxPos - c.minPos)
                  }//if it's the last part in contig or the last at all
                  else {
                    u.startPoint + u.cov.length - c.minPos  + 1
                  }

                logger.debug(s"##### Overlap length ${overlapLength} for ${it} from ${u.contigName},${u.minPos}, ${u.startPoint},${u.cov.length}")
                shrinkMap.get((u.contigName, u.minPos)) match {
                  case Some(s) => shrinkMap.update((u.contigName, u.minPos), math.min(s,c.minPos - u.minPos + 1))
                  case _ =>  shrinkMap += (u.contigName, u.minPos) -> (c.minPos - u.minPos + 1)
                }
                updateMap.get((c.contigName, c.minPos)) match {
                  case Some(up) =>
                    val arr = Array.fill[Short](math.max(0,u.startPoint-c.minPos))(0) ++ u.cov.takeRight(overlapLength)
                    val newArr=up._1.get.zipAll(arr, 0.toShort ,0.toShort).map{ case (x, y) => (x + y).toShort }
                    val newCumSum= (up._2 - u.cov.takeRight(overlapLength).sum).toShort

                    logger.debug(s"overlap u.minPos=${u.minPos} len = ${newArr.length}")
                    logger.debug(s"next update to updateMap: c.minPos=${c.minPos}, u.minPos=${u.minPos} curr_len${up._1.get.length}, new_len = ${newArr.length}")

//                    updateMap.update(
//                    (u.contigName, u.minPos),
//                    (Some(up._1
//                      .get
//                      .zipAll(Array.fill[Short](math.max(0,u.startPoint-c.minPos))(0) ++ u.cov.takeRight(overlapLength), 0.toShort, 0.toShort) //first drop abovr part maxPos and take the overlap
//                      .map { case (x, y) => (x + y).toShort }), //merge coverage
//                      (up._2 - u.cov.takeRight(overlapLength).sum).toShort ) // delete anything that is > c.minPos

                    if(u.minPos < c.minPos)
                      updateMap.update((c.contigName, c.minPos), (Some(newArr), newCumSum))
                    else
                      updateMap.update((c.contigName, u.minPos), (Some(newArr), newCumSum)) // delete anything that is > c.minPos
                  case _ => {
                    logger.debug(s"overlap u.minPos=${u.minPos} u.max = ${u.minPos + overlapLength - 1} len = ${overlapLength}")
                    logger.debug(s"first update to updateMap: ${math.max(0,u.startPoint-c.minPos)}, $overlapLength ")
                    updateMap += (c.contigName, c.minPos) -> (Some(Array.fill[Short](math.max(0,u.startPoint-c.minPos))(0) ++u.cov.takeRight(overlapLength)), (cumSum - u.cov.takeRight(overlapLength).sum).toShort)
                  }
                }
              }
            logger.debug(s"#### Update struct length: ${updateMap(c.contigName, c.minPos)._1.get.length}")
            logger.debug(s"#### Shrinking struct ${shrinkMap.mkString("|")}")
            }
            else {
              updateMap += (c.contigName, c.minPos) -> (None, cumSum)

          }
          if (c.minPos < minmax(contig)._1)
            minmax(contig) = (c.minPos, minmax(contig)._2)
          if (c.maxPos > minmax(contig)._2)
            minmax(contig) = (minmax(contig)._1, c.maxPos)
        it += 1
      }
      logger.debug(s"Prepared broadcast $updateMap, $shrinkMap")

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

    //check if it's a window length or a table name
    val maybeWindowLength =
      try {
              target match {
                case Some(t) => Some(t.toInt)
                case _ => None
              }
      } catch {
        case e: Exception => None
    }


    lazy val cov =
      if(maybeWindowLength != None) //fixed-length window
        CoverageMethodsMos.eventsToCoverage(sampleId, reducedEvents, covBroad.value.minmax, blocksResult, allPos,maybeWindowLength,None)

          .keyBy(_.key)
          .reduceByKey((a,b) =>
            CovRecordWindow(a.contigName,
              a.start,
              a.end,
              (a.asInstanceOf[CovRecordWindow].overLap.get * a.asInstanceOf[CovRecordWindow].cov + b.asInstanceOf[CovRecordWindow].overLap.get * b.asInstanceOf[CovRecordWindow].cov )/
                (a.asInstanceOf[CovRecordWindow].overLap.get + b.asInstanceOf[CovRecordWindow].overLap.get),
              Some(a.asInstanceOf[CovRecordWindow].overLap.get + b.asInstanceOf[CovRecordWindow].overLap.get) ) )
          .map(_._2)

       else
        CoverageMethodsMos.eventsToCoverage(sampleId, reducedEvents, covBroad.value.minmax, blocksResult, allPos,None, None)

    if(maybeWindowLength != None) {   // windows

      cov.mapPartitions(p => {
        val proj = UnsafeProjection.create(schema)
        p.map(r => proj.apply(InternalRow.fromSeq(Seq(
          UTF8String.fromString(r.contigName), r.start, r.end, r.asInstanceOf[CovRecordWindow].cov))))
      })
    } else { // regular blocks
      cov.mapPartitions(p => {
        val proj = UnsafeProjection.create(schema)
        p.map(r => proj.apply(InternalRow.fromSeq(Seq(/*UTF8String.fromString(sampleId),*/
          UTF8String.fromString(r.contigName), r.start, r.end, r.asInstanceOf[CovRecord].cov))))
      })
    }

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

