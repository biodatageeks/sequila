package org.biodatageeks.sequila.pileup

import java.io.{File, OutputStreamWriter, PrintWriter}
import java.util

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.samtools.{CigarOperator, SAMRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.sequila.pileup.model._
import org.biodatageeks.sequila.pileup.timers.PileupTimers._
import org.biodatageeks.sequila.utils.{DataQualityFuncs, FastMath, InternalParams, SequilaRegister}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConverters, mutable}

/**
  * Class implementing pileup calculations on set of aligned reads
  */
object PileupMethods {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  /**
    * implementation of pileup algorithm
    *
    * @param alignments aligned reads
    * @param spark spark session
    * @return distributed collection of PileupRecords
    */
  def calculatePileup(alignments: RDD[SAMRecord], spark: SparkSession, refPath: String): RDD[InternalRow] = {

    val enableInstrumentation = spark
      .sqlContext
      .getConf(InternalParams.EnableInstrumentation).toBoolean
    val alignmentsInstr = if(enableInstrumentation) alignments.instrument() else alignments
    val aggregates = ContigAggrTimer.time {
      assembleContigAggregates(alignmentsInstr)
        .persist(StorageLevel.MEMORY_AND_DISK_SER) //FIXME: Add automatic unpersist
    }
    val accumulator = AccumulatorTimer.time {
      accumulateTails(aggregates, spark)
    }

    val broadcast = BroadcastTimer.time{ spark.sparkContext.broadcast(prepareOverlaps(accumulator.value())) }

    val adjustedEvents = AdjustedEventsTimer.time {adjustEventAggregatesWithOverlaps(aggregates, broadcast) }
    val pileup =
      eventsToPileup(adjustedEvents, refPath)

    pileup
    //      .count()
    //    adjustedEvents.count
    //    spark.sparkContext.parallelize(Array(InternalRow()))
  }

  /**
    * Collects "interesting" (read start, stop, ref/nonref counting) events on alignments
    *
    * @param alignments aligned reads
    * @return distributed collection of PileupRecords
    */
  def assembleContigAggregates(alignments: RDD[SAMRecord]): RDD[ContigEventAggregate] = {
    val contigLenMap = InitContigLengthsTimer.time  {
      initContigLengths(alignments.first())
    }

    alignments.mapPartitions { partition =>
      val aggMap = new mutable.HashMap[String, ContigEventAggregate]()
      val contigMaxReadLen = new mutable.HashMap[String, Int]()
      var contigIter  = ""
      var contigCleanIter  = ""
      MapPartitionTimer.time {
        while (partition.hasNext) {
          val read = BAMReadTimer.time {
            partition.next()
          }
          val contig = DQTimerTimer.time {
            if(read.getContig == contigIter)
              contigCleanIter
            else {
              contigIter = read.getContig
              contigCleanIter =  DataQualityFuncs.cleanContig(contigIter)
              contigCleanIter
            }
          }

          if (!aggMap.contains(contig))
            HandleFirstContingTimer.time {
              handleFirstReadForContigInPartition(read, contig, contigLenMap, contigMaxReadLen, aggMap)
            }
          val contigEventAggregate = AggMapLookupTimer.time {
            aggMap(contig)
          }

          AnalyzeReadsTimer.time {
            analyzeRead(read, contig, contigEventAggregate, contigMaxReadLen)
          }
        }
        PrepareOutupTimer.time {
          prepareOutputAggregates(aggMap, contigMaxReadLen)
            .toIterator
        }
      }
    }
  }



  /**
    * gathers "tails" of events array that might be overlapping with other partitions. length of tail is equal to the
    * longest read in this aggregate
    * @param events events array with additional aggregate information from partition
    * @return
    */

  private def accumulateTails(events: RDD[ContigEventAggregate], spark:SparkSession): PileupAccumulator = {

    val accumulator = AccumulatorAllocTimer.time {new PileupAccumulator(new PileupUpdate(new ArrayBuffer[TailEdge](), new ArrayBuffer[ContigRange]())) }
    AccumulatorRegisterTimer.time {spark.sparkContext.register(accumulator, name="PileupAcc") }

    events foreach {
      agg => {
        AccumulatorNestedTimer.time {
          val contig = agg.contig
          val minPos = agg.startPosition
          val maxPos = agg.maxPosition
          val maxSeqLen = agg.maxSeqLen

          val tailStartPos = maxPos - maxSeqLen
          val tailCov = TailCovTimer.time {
            if(agg.maxSeqLen ==  agg.events.length ) agg.events
            else
              agg.events.takeRight(agg.maxSeqLen)
          }
          val tailAlts = TailAltsTimer.time { agg.alts.filter(_._1 >= tailStartPos) } // verify >= or >


          val tail = TailEdgeTimer.time {

            TailEdge(contig, minPos, tailStartPos, tailCov, tailAlts, FastMath.sumShort(agg.events))
          }
          val range = ContigRange(contig, minPos, maxPos)
          val pileupUpdate = new PileupUpdate(ArrayBuffer(tail), ArrayBuffer(range))
          if (logger.isDebugEnabled()) logger.debug(s"Adding record for: chr=$contig,start=$minPos,end=$maxPos,span=${maxPos - minPos + 1},maxSeqLen=$maxSeqLen")
          accumulator.add(pileupUpdate)
        }
      }
    }
    accumulator
  }

  /**
    * calculate actual overlaps between slices. They will be then broadcasted.
    * @param update - structure containing tails of slices and their contig ranges
    * @return - structure for broadcast
    */
  def prepareOverlaps(update: PileupUpdate): UpdateStruct = {
    val tails = update.tails
    val ranges = update.ranges
    val updateMap = new mutable.HashMap[(String, Int), (Option[Array[Short]], Option[mutable.HashMap[Int, mutable.HashMap[Byte,Short]]], Short)]()
    val shrinkMap = new mutable.HashMap[(String, Int), Int]()
    val minmax = new mutable.HashMap[String, (Int, Int)]()

    var it = 0
    for (range <- ranges.sortBy(r => (r.contig, r.minPos))) {
      if (!minmax.contains(range.contig))
        minmax += range.contig -> (Int.MaxValue, 0)

      val overlaps = findOverlappingTailsForRange(range, tails)
      val cumSum = precedingCumulativeSum(range, tails)

      if(overlaps.isEmpty)
        updateMap += (range.contig, range.minPos) -> (None, None, cumSum)
      else { // if there are  overlaps for this contigRange
        for(o <- overlaps) {
          val overlapLength = calculateOverlapLength(o, range, it, ranges)
          updateShrinksByOverlap(o, range, shrinkMap)
          updateUpdateByOverlap(o, overlapLength, range, cumSum, updateMap)
        }
        if(logger.isDebugEnabled())  logger.debug(s"UpdateStruct len:${updateMap(range.contig,range.minPos)._1.get.length}, shrinkmap ${shrinkMap.mkString("|")}")
      }
      updateMinMaxByOverlap(range, minmax)
      it += 1
    }
    if(logger.isDebugEnabled())  logger.debug(s"Prepared broadcast $updateMap, $shrinkMap")
    UpdateStruct(updateMap, shrinkMap, minmax)
  }

  /**
    * return new contigEventsAggregates with events array taking overlaps into account
    * @param events
    * @param b
    * @return
    */

  def adjustEventAggregatesWithOverlaps(events: RDD[ContigEventAggregate],
                                        b: Broadcast[UpdateStruct])
  : RDD[ContigEventAggregate] = {
    if(logger.isDebugEnabled())  logger.debug("Updating aggregates. Agg count={}", events.count)

    val upd: mutable.HashMap[(String, Int), (Option[Array[Short]], Option[mutable.HashMap[Int, mutable.HashMap[Byte,Short]]], Short)] = b.value.upd
    val shrink = b.value.shrink

    events map { agg => {

      val updatedEventsArray = CalculateEventsTimer.time { calculateEventsArrayWithBroadcast(agg, upd) }
      val updatedAltsMap = CalculateAltsTimer.time{ calculateAltsWithBroadcast(agg,upd) }

      //val shrinkedEventsArray = ShrinkArrayTimer.time { shrinkArrayWithBroadcast(agg, shrink, updatedEventsArray) }
      val shrinkedEventsArraySize = ShrinkArrayTimer.time { getShrinkSizeWithBroadCast(agg, shrink, updatedEventsArray) }
      val shrinkedAltsMap = ShrinkAltsTimer.time { shrinkAltsMapWithBroadcast(agg, shrink, updatedAltsMap) }
      if(logger.isDebugEnabled())  logger.debug(
        s"End of Update Partition: $agg.contig, min=${agg.startPosition} max=${agg.maxPosition} len:${agg.events.length} span: ${agg.maxPosition - agg.startPosition}")
      ContigEventAggregate(agg.contig, agg.contigLen, updatedEventsArray, shrinkedAltsMap,  agg.startPosition, agg.maxPosition, shrinkedEventsArraySize)
    }}
  }



  @inline private def updateMinMaxByOverlap(range: ContigRange, minmax: mutable.HashMap[String, (Int, Int)]): Unit = {
    if (range.minPos < minmax(range.contig)._1)
      minmax(range.contig) = (range.minPos, minmax(range.contig)._2)
    if (range.maxPos > minmax(range.contig)._2)
      minmax(range.contig) = (minmax(range.contig)._1, range.maxPos)
  }

  def mergeMaps(map1: mutable.HashMap[Byte, Short], map2: mutable.HashMap[Byte, Short]): mutable.HashMap[Byte, Short] = {
    if (map1 == null)
      return map2
    if (map2 == null)
      return map1
    val keyset = map1.keySet++map2.keySet
    val mergedMap = new mutable.HashMap[Byte, Short]()
    for (k <- keyset)
      mergedMap(k) = (map1.getOrElse(k, 0.toShort) + map2.getOrElse(k, 0.toShort)).toShort
    mergedMap
  }

  def mergeAltsMaps(map1: mutable.HashMap[Int, mutable.HashMap[Byte,Short]], map2: mutable.HashMap[Int, mutable.HashMap[Byte,Short]]): mutable.HashMap[Int, mutable.HashMap[Byte,Short]] = {
    if (map1 == null)
      return map2
    if (map2 == null)
      return map1
    val keyset = map1.keySet++map2.keySet
    var mergedAltsMap = mutable.HashMap.empty[Int, mutable.HashMap[Byte,Short]]
    for (k <- keyset)
      mergedAltsMap += k -> mergeMaps(map1.getOrElse(k,null), map2.getOrElse(k,null)) // to refactor
    mergedAltsMap
  }

  private def updateUpdateByOverlap(o: TailEdge, overlapLength: Int, range: ContigRange, cumSum: Short, updateMap: mutable.HashMap[(String, Int), (Option[Array[Short]], Option[mutable.HashMap[Int, mutable.HashMap[Byte,Short]]], Short)]) = {
    updateMap.get((range.contig, range.minPos)) match {
      case Some(up) =>
        val arrEvents = Array.fill[Short](math.max(0, o.startPoint - range.minPos))(0) ++ o.events.takeRight(overlapLength)
        val newArrEvents = up._1.get.zipAll(arrEvents, 0.toShort, 0.toShort).map { case (x, y) => (x + y).toShort }

        val newMap = up._2.get ++ o.alts
        val newAlts= newMap.asInstanceOf[mutable.HashMap[Int, mutable.HashMap[Byte,Short]]]


        val newCumSum = (up._3 - FastMath.sumShort(o.events.takeRight(overlapLength)) ).toShort

        if(logger.isDebugEnabled()) logger.debug(s"overlap o.minPos=${o.minPos} len = ${newArrEvents.length}")
        if(logger.isDebugEnabled()) logger.debug(s"next update to updateMap: range.minPos=${range.minPos}, o.minPos=${o.minPos} curr_len${up._1.get.length}, new_len = ${newArrEvents.length}")

        if (o.minPos < range.minPos)
          updateMap.update((range.contig, range.minPos), (Some(newArrEvents), Some(newAlts), newCumSum))
        else
          updateMap.update((range.contig, o.minPos), (Some(newArrEvents), Some(newAlts), newCumSum)) // delete anything that is > range.minPos
      case _ =>
        if(logger.isDebugEnabled())  logger.debug(s"overlap o.minPos=${o.minPos} o.max = ${o.minPos + overlapLength - 1} len = $overlapLength")
        if(logger.isDebugEnabled())  logger.debug(s"first update to updateMap: ${math.max(0, o.startPoint - range.minPos)}, $overlapLength ")
        updateMap +=
          (range.contig, range.minPos) ->
            (
              Some(Array.fill[Short](math.max(0, o.startPoint - range.minPos))(0) ++ o.events.takeRight(overlapLength)),
              Some(o.alts),
              (cumSum - FastMath.sumShort(o.events.takeRight(overlapLength)) ).toShort
            )
    }
  }

  @inline private def updateShrinksByOverlap(o: TailEdge, range: ContigRange, shrinkMap: mutable.HashMap[(String, Int), Int]) = {
    shrinkMap.get((o.contig, o.minPos)) match {
      case Some(s) => shrinkMap.update((o.contig, o.minPos), math.min(s, range.minPos - o.minPos + 1))
      case _ => shrinkMap += (o.contig, o.minPos) -> (range.minPos - o.minPos + 1)
    }
  }


  @inline private def calculateOverlapLength(o: TailEdge, range: ContigRange, it: Int, ranges: ArrayBuffer[ContigRange]) = {
    val length = if ((o.startPoint + o.events.length) > range.maxPos && ((ranges.length - 1 == it) || ranges(it + 1).contig != range.contig)) {
      o.startPoint + o.events.length - range.minPos + 1
    }
    else if ((o.startPoint + o.events.length) > range.maxPos) {
      range.maxPos - range.minPos
    } //if it's the last part in contig or the last at all
    else {
      o.startPoint + o.events.length - range.minPos + 1
    }
    if(logger.isDebugEnabled())  logger.debug("Overlap length $l for $it from ${o.contig},${o.minPos}, ${o.startPoint},${o.cov.length}")
    length

  }

  @inline private def precedingCumulativeSum(range:ContigRange, tails: ArrayBuffer[TailEdge]): Short =
    FastMath.sumShort(tails
      .filter(t => t.contig == range.contig && t.minPos < range.minPos)
      .map(_.cumSum).toArray
    )


  @inline private def findOverlappingTailsForRange(range: ContigRange, tails: ArrayBuffer[TailEdge]) = {
    tails
      .filter(t => (t.contig == range.contig && t.startPoint + t.events.length > range.minPos) && t.minPos < range.minPos)
      .sortBy(r => (r.contig, r.minPos))
  }

  @inline
  def calculateMaxLength(agg: ContigEventAggregate, allPositions: Boolean): Int = {
    if (! allPositions)
      return agg.events.length

    val firstBlockMaxLength = agg.startPosition - 1
    val lastBlockMaxLength = agg.contigLen - agg.maxPosition
    firstBlockMaxLength + agg.events.length + lastBlockMaxLength

  }


  def addBaseRecord(result:Array[InternalRow], ind:Int,
                    agg:ContigEventAggregate, bases:String, i:Int, prev:BlockProperties) {
    val groupRef = bases.substring(prev.pos, i)
    result(ind) = createBasePileupRow( prev.alt, agg, groupRef, prev.cov, i)
    prev.alt.clear()
  }
  def addBlockRecord(result:Array[InternalRow], ind:Int,
                     agg:ContigEventAggregate, bases:String, i:Int, prev:BlockProperties) {
    val groupRef = bases.substring(prev.pos, i)
    result(ind) = createBlockPileupRow( agg, groupRef, prev.cov, i, prev.len)
  }


  /**
    * Convert events array to pileup. Shows only non-zero coverage
    * Currently fixed constants added as REF, COUNTREF, COUNTNONREF
    *
    * @param events events aggregates
    * @return rdd of pileup records
    */
  def eventsToPileup(events: RDD[ContigEventAggregate], refPath: String): RDD[InternalRow] = {
    events.mapPartitions { part =>
      val reference = new IndexedFastaSequenceFile(new File(refPath))
      val contigMap = getNormalizedContigMap(reference)
      PileupProjection.setContigMap(contigMap)

      part.map { agg => {
        var cov, ind, i = 0
        val allPos = false
        val maxLen = calculateMaxLength(agg, allPos)
        val result = new Array[InternalRow](maxLen)
        val prev = new BlockProperties()

        val bases = EventsGetBasesTimer.time {
          getBasesFromReference(reference, contigMap(agg.contig), agg.startPosition, agg.startPosition + agg.events.length - 1)
        }
        while (i < agg.shrinkedEventsArraySize) {
          cov += agg.events(i)

          if (prev.hasAlt) {
            addBaseRecord(result, ind, agg, bases, i, prev)
            ind += 1;
            prev.reset(i)
            if (agg.hasAltOnPosition(i))
              prev.alt=agg.alts(i)
          }
          else if (agg.hasAltOnPosition(i)) { // there is ALT in this posiion
            if (prev.isNonZeroCoverage) { // there is previous group non-zero group -> convert it
              addBlockRecord(result, ind, agg, bases, i, prev)
              ind += 1;
              prev.reset(i)
            } else if (prev.isZeroCoverage) // previous ZERO group, clear block
            prev.reset(i)
            prev.alt = agg.alts(i)
          }
          else if (isEndOfZeroCoverageRegion(cov, prev.cov, i)) { // coming back from zero coverage. clear block
            prev.reset(i)
          }
          else if (isChangeOfCoverage(cov, prev.cov, i) || isStartOfZeroCoverageRegion(cov, prev.cov)) { // different cov, add to output previous group
            addBlockRecord(result, ind, agg, bases, i, prev)
            ind += 1;
            prev.reset(i)
          }
          prev.cov = cov;
          prev.len = prev.len + 1;
          i += 1
        }
        if (ind < maxLen) result.take(ind) else result
      }
      }
    }.flatMap(r => r)
  }

  private def isStartOfZeroCoverageRegion(cov: Int, prevCov: Int) = {
    cov == 0 && prevCov > 0
  }

  private def isChangeOfCoverage(cov: Int, prevCov: Int, i: Int) = {
    cov != 0 && prevCov >= 0 && prevCov != cov && i > 0
  }


  private def previousGroupIsZeroCoverage(prevCov: Int, blockLength: Int) = {
    blockLength != 0 && prevCov == 0
  }

  private def previousGroupIsNonZeroCoverage(prevCov: Int, blockLength: Int) = {
    blockLength != 0 && prevCov != 0
  }

  private def isEndOfZeroCoverageRegion(cov: Int, prevCov: Int, i: Int) = {
    cov != 0 && prevCov == 0 && i > 0
  }

  private def existsAltOnPositon(agg: ContigEventAggregate, position:Int):Boolean = agg.alts.contains(position)


  private def createBasePileupRow(map: mutable.HashMap[Byte,Short], agg: ContigEventAggregate, ref: String, cov: Int, i: Int ) = {
    val altsCount = map.foldLeft(0)(_ + _._2).toShort
    val row = PileupProjection.convertToRow(agg.contig, i + agg.startPosition -1, i + agg.startPosition-1, ref, cov.toShort, (cov - altsCount).toShort, altsCount.toShort, map.toMap)
    row
  }

  private def createBlockPileupRow(agg: ContigEventAggregate, ref: String, prevCov: Int, i: Int, blockLength: Int) = {
    CreateBlockPileupRowTimer.time {
      val row = PileupProjection.convertToRow(agg.contig, i + agg.startPosition - blockLength, i + agg.startPosition - 1, ref, prevCov.toShort, prevCov.toShort, 0.toShort, null)
      row
    }
  }

  private def getNormalizedContigMap(fasta: IndexedFastaSequenceFile) = {
    val normContigMap = new mutable.HashMap[String, String]()
    val iter = fasta.getIndex.iterator()
    while (iter.hasNext){
      val contig = iter.next().getContig
      normContigMap += (DataQualityFuncs.cleanContig(contig) -> contig )
    }
    normContigMap
  }
  def getBaseFromReference(fasta: IndexedFastaSequenceFile, contigMap: mutable.HashMap[String,String], contig: String, index: Int): String = {

    val refBase = fasta.getSubsequenceAt(contigMap(contig), index.toLong, index.toLong)
    refBase.getBaseString.toUpperCase
  }

  def getBasesFromReference(fasta: IndexedFastaSequenceFile, contig: String, startIndex: Int, endIndex: Int): String = {
    val refBases = fasta.getSubsequenceAt(contig, startIndex.toLong, endIndex.toLong-1)
    refBases.getBaseString.toUpperCase
  }


  def calculateEventsArrayWithBroadcast(agg: ContigEventAggregate, upd: mutable.HashMap[(String, Int), (Option[Array[Short]], Option[mutable.HashMap[Int, mutable.HashMap[Byte,Short]]], Short)]): Array[Short] = {
    var eventsArrMutable = agg.events

    upd.get((agg.contig, agg.startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
      case Some((arrEvents, arrAlt, covSum)) => // array of covs and cumSum
        arrEvents match {
          case Some(overlapArray) =>
            if (overlapArray.length > agg.events.length)
              eventsArrMutable =  agg.events ++ Array.fill[Short](overlapArray.length -  agg.events.length)(0) // extend array

            var i = 0
            //logger.debug(s"$agg.contig, min=${agg.startPosition} max=${agg.maxPosition} updating: ${eventsArrMutable
            // .take(10).mkString(",")} with ${overlapArray.take(10).mkString(",")} and $covSum ")
            eventsArrMutable(i) = (eventsArrMutable(i) + covSum).toShort // add cumSum to zeroth element

            while (i < overlapArray.length) {
              try {
                eventsArrMutable(i) = (eventsArrMutable(i) + overlapArray(i)).toShort
                i += 1
              } catch {
                case e: ArrayIndexOutOfBoundsException =>
                  logger.error(
                    s" Overlap array length: ${overlapArray.length}, events array length: ${agg.events.length}")
                  throw e
              }
            }
            if(logger.isDebugEnabled()) logger.debug(s"$agg.contig, min=${agg.startPosition} max=${agg.maxPosition} Updated array ${eventsArrMutable.take(10).mkString(",")}")
            eventsArrMutable
          case None =>
            eventsArrMutable(0) = (eventsArrMutable(0) + covSum).toShort
            eventsArrMutable
        }
      case None =>
        eventsArrMutable
    }
  }

  def calculateAltsWithBroadcast(agg: ContigEventAggregate, upd: mutable.HashMap[(String, Int), (Option[Array[Short]], Option[mutable.HashMap[Int, mutable.HashMap[Byte,Short]]], Short)]): mutable.HashMap[Int, mutable.HashMap[Byte,Short]] = {
    upd.get((agg.contig, agg.startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
      case Some((_, alts, _)) => // covs alts cumSum
        alts match {
          case Some(overlapAlts) =>
            mergeAltsMaps(agg.alts, overlapAlts)
          case None =>
            agg.alts
        }
      case None =>
        agg.alts
    }
  }


  private def shrinkArrayWithBroadcast[T](agg: ContigEventAggregate, shrink: mutable.HashMap[(String, Int), Int], updArray: Array[T]):Array[T] = {
    shrink.get((agg.contig, agg.startPosition)) match {
      case Some(len) =>
        if(updArray.length > len) updArray.take(len) else updArray
      case None => updArray
    }
  }

  private def getShrinkSizeWithBroadCast[T](agg: ContigEventAggregate,shrink: mutable.HashMap[(String, Int), Int], updArray: Array[T]): Int = {
    shrink.get((agg.contig, agg.startPosition)) match {
      case Some(len) => len
      case None => updArray.length
    }
  }


  private def shrinkAltsMapWithBroadcast[T](agg: ContigEventAggregate, shrink: mutable.HashMap[(String, Int), Int], altsMap: mutable.HashMap[Int, mutable.HashMap[Byte,Short]]): mutable.HashMap[Int, mutable.HashMap[Byte,Short]] = {
    shrink.get((agg.contig, agg.startPosition)) match {
      case Some(len) =>
        val cutoffPosition = agg.maxPosition- len
        altsMap.filter(_._1 > cutoffPosition)
      case None => altsMap
    }
  }


  private def handleFirstReadForContigInPartition(read: SAMRecord, contig: String, contigLenMap: Map[String, Int],
                                                  contigMaxReadLen: mutable.HashMap[String, Int],
                                                  aggMap: mutable.HashMap[String, ContigEventAggregate]) = {
    val contigLen = contigLenMap(contig)
    val arrayLen = contigLen - read.getStart + 10

    val contigEventAggregate = ContigEventAggregate(
      contig = contig,
      contigLen = contigLen,
      events = new Array[Short](arrayLen),
      alts = mutable.HashMap.empty[Int, mutable.HashMap[Byte,Short]],
      startPosition = read.getStart,
      maxPosition = contigLen - 1)

    aggMap += contig -> contigEventAggregate
    contigMaxReadLen += contig -> 0
  }

  /**
    * simply updates the map between contig and max read length (for overlaps). If current read len is greater update map
    *
    * @param read             analyzed aligned read from partition
    * @param contig           read contig (cleaned)
    * @param contigMaxReadLen map between contig and max read length
    */
  @inline
  private def updateMaxReadLenInContig(read: SAMRecord, contig: String, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {
    val seqLen = read.getReadLength
    if (seqLen > contigMaxReadLen(contig))
      contigMaxReadLen(contig) = seqLen
  }

  @inline
  private def updateMaxCigarInContig(cigarLen:Int, contig: String, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {
    if (cigarLen > contigMaxReadLen(contig))
      contigMaxReadLen(contig) = cigarLen
  }


  /**
    * updates events array for contig and updates contig's max read length
    *
    * @param read             analyzed aligned read from partition
    * @param contig           read contig (cleaned)
    * @param aggregate   object holding current state of events aggregate in this contig
    * @param contigMaxReadLen map between contig and max read length (for overlaps)
    */
  def analyzeRead(read: SAMRecord, contig: String,
                  aggregate: ContigEventAggregate,
                  contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {

    AnalyzeReadsCalculateEventsTimer.time { calculateEvents(read, contig, aggregate) }
    AnalyzeReadsCalculateAltsTimer.time{ calculateAlts(read, aggregate) }
    AnalyzeReadsUpdateMaxReadLenInContigTimer.time{ updateMaxReadLenInContig(read, contig, contigMaxReadLen) }
  }


  def calculateEvents(read: SAMRecord, contig: String, aggregate: ContigEventAggregate): Unit = {

    val partitionStart = aggregate.startPosition
    var position = read.getStart
    val cigarIterator = read.getCigar.iterator()
    var cigarLen = 0

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator

      if (cigarOperator == CigarOperator.M ||
        cigarOperator == CigarOperator.X   ||
        cigarOperator == CigarOperator.EQ  ||
        cigarOperator == CigarOperator.N   ||
        cigarOperator == CigarOperator.D)
        cigarLen += cigarOperatorLen

      // update events array according to read alignment blocks start/end
      if (cigarOperator == CigarOperator.M || cigarOperator == CigarOperator.X || cigarOperator == CigarOperator.EQ) {

        updateContigEventsArray(position, partitionStart, contig, aggregate, delta = 1)
        position += cigarOperatorLen
        updateContigEventsArray(position, partitionStart, contig, aggregate, delta = -1)
      }
      else if (cigarOperator == CigarOperator.N || cigarOperator == CigarOperator.D)
        position += cigarOperatorLen

    }


    //    updateMaxCigarInContig(cigarLen, contig, contigMaxReadLen)

  }

  private def getAltBaseFromSequence(read: SAMRecord, position: Int):Char = {
    read.getReadString.charAt(position-1) // FIXME - verify -1
  }

  def calculatePositionInReadSeq(read: SAMRecord, mdPosition: Int): Int = {
    val cigar = read.getCigar
    if (!cigar.containsOperator(CigarOperator.INSERTION))
      return mdPosition

    var cnt = 0
    var numInsertions = 0
    val cigarIterator = cigar.iterator()
    var position = 0

    while (cigarIterator.hasNext){
      if (position > mdPosition + numInsertions)
        return mdPosition + numInsertions
      val cigarElement = cigarIterator.next()
      val cigarOpLength = cigarElement.getLength
      val cigarOp = cigarElement.getOperator

      if (cigarOp == CigarOperator.INSERTION) {
        numInsertions += cigarOpLength
      }

      position = position+cigarOpLength
    }
    mdPosition + numInsertions
  }

  private def isNonDeletion (mdtag: MDOperator):Boolean = mdtag.base.isUpper

  private def isDeletion (mdtag: MDOperator):Boolean = mdtag.base.isLower

  private def calculateAlts(read: SAMRecord, eventAggregate: ContigEventAggregate): Unit = {
    val partitionStart = eventAggregate.startPosition
    var position = read.getStart
    val md = read.getAttribute("MD").toString
    val ops = MDTagParser.parseMDTag(md)
    var delCounter = 0
    val clipLen =
      if (read.getCigar.getCigarElement(0).getOperator.isClipping)
        read.getCigar.getCigarElement(0).getLength else 0

    position += clipLen

    for (mdtag <- ops) {
      if (isDeletion(mdtag)) {
        delCounter += 1
        position +=1
      } else if (mdtag.base != 'S') {
        //        if (isDeletion(mdtag))
        //          delCounter += 1
        position += 1

        val indexInSeq = calculatePositionInReadSeq(read, position - read.getStart -delCounter)
        val altBase = getAltBaseFromSequence(read, indexInSeq)
        updateContigAltsMap(position - clipLen, partitionStart, eventAggregate, altBase)
      }
      else if(mdtag.base == 'S')
        position += mdtag.length
    }
  }


  /**
    * transforms map structure of contigEventAggregates, by reducing number of last zeroes in the cov array
    * also adds calculated maxCigar len to output
    *
    * @param aggMap   mapper between contig and contigEventAggregate
    * @param cigarMap mapper between contig and max length of cigar in given
    * @return
    */
  def prepareOutputAggregates(aggMap: mutable.HashMap[String, ContigEventAggregate], cigarMap: mutable.HashMap[String, Int]): Array[ContigEventAggregate] = {
    val output = new Array[ContigEventAggregate](aggMap.size)
    var i = 0
    val iter = aggMap.toIterator
    while(iter.hasNext){
      val nextVal = iter.next()
      val contig = nextVal._1
      val contigEventAgg = nextVal._2

      val maxIndex: Int = findMaxIndex(contigEventAgg.events)
      val agg = ContigEventAggregate(
        contig,
        contigEventAgg.contigLen,
        util.Arrays.copyOfRange(contigEventAgg.events, 0, maxIndex + 1), //FIXME: https://stackoverflow.com/questions/37969193/why-is-array-slice-so-shockingly-slow
        contigEventAgg.alts,
        contigEventAgg.startPosition,
        contigEventAgg.startPosition + maxIndex,
        cigarMap(contig))
      output(i) = agg
      i += 1
    }
    output
  }

  /**
    * updates events array for contig. Should be invoked with delta = 1 for alignment start and -1 for alignment stop
    *
    * @param pos            - position to be changed
    * @param startPart      - starting position of partition (offset)
    * @param contig         - contig
    * @param eventAggregate - aggregate
    * @param delta          - value to be added to position
    */

  @inline
  def updateContigEventsArray(
                               pos: Int,
                               startPart: Int,
                               contig: String,
                               eventAggregate: ContigEventAggregate,
                               delta: Short): Unit = {
    val position = pos - startPart
    eventAggregate.events(position) = (eventAggregate.events(position) + delta).toShort
  }


  @inline
  def updateContigAltsMap(
                           pos: Int,
                           startPart: Int,
                           eventAggregate: ContigEventAggregate,
                           alt: Char): Unit = {

    val position = pos - startPart - 1
    val altByte = alt.toByte

    if (eventAggregate.alts.contains(position)) {
      val altMap = eventAggregate.alts(position)
      if (altMap.contains(altByte)) {
        val prevAltCount = altMap(altByte)
        eventAggregate.alts(position)(altByte) = (prevAltCount + 1).toShort
      }
      else
        eventAggregate.alts(position) += (altByte -> 1)
    }
    else
      eventAggregate.alts(position) = mutable.HashMap(altByte -> 1)

    assert(eventAggregate.alts(position) != null)
  }

  /**
    * initializes mapper between contig and its length basing on header values
    *
    * @param read single aligned read (its header contains info about all contigs)
    * @return
    */
  def initContigLengths(read: SAMRecord): Map[String, Int] = {
    val contigLenMap = new mutable.HashMap[String, Int]()

    val sequenceList = read.getHeader.getSequenceDictionary.getSequences
    val sequenceSeq = JavaConverters.asScalaIteratorConverter(sequenceList.iterator()).asScala.toSeq

    for (sequence <- sequenceSeq) {
      if(logger.isDebugEnabled())  logger.debug("sequence name {} => {} ", sequence.getSequenceName, sequence.getSequenceLength)
      val contigName = DataQualityFuncs.cleanContig(sequence.getSequenceName)
      contigLenMap += contigName -> sequence.getSequenceLength
    }
    contigLenMap.toMap
  }

  /**
    * finds index of the last non zero element of array
    * @param array array of Short elements
    * @return index
    */

  def findMaxIndex(array: Array[Short]): Int = {
    var i = array.length - 1

    while (i > 0) {
      if (array(i) != 0)
        return i
      i -= 1
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.driver.memory","4g")
      .config( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .enableHiveSupport()
      .getOrCreate()

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    spark.sparkContext.setLogLevel("INFO")
    val bamPath = "/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.md.bam"
    //    val bamPath = "/Users/marek/data/NA12878.proper.wes.md.bam"
    val referencePath = "/Users/marek/data/hs37d5.fa"
    //    val referencePath = "/Users/marek/data/Homo_sapiens_assembly18.fasta"
    val tableNameBAM = "reads"

    ss.sql(s"""DROP  TABLE IF  EXISTS $tableNameBAM""")
    ss.sql(s"""
              |CREATE TABLE $tableNameBAM
              |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
              |OPTIONS(path "$bamPath")
              |
      """.stripMargin)


    val query =
      s"""
         |SELECT count(*)
         |FROM  pileup('$tableNameBAM', '${referencePath}')
       """.stripMargin

    ss
      .sqlContext
      .setConf(InternalParams.EnableInstrumentation, "true")
    Metrics.initialize(ss.sparkContext)
    val metricsListener = new MetricsListener(new RecordedMetrics())
    ss
      .sparkContext
      .addSparkListener(metricsListener)
    val results = ss.sql(query)
    ss.time{
      results.show()
    }


    val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.close()

    ss.stop()
  }



}
