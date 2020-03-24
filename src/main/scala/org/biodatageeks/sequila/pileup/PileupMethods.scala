package org.biodatageeks.sequila.pileup

import htsjdk.samtools.{CigarOperator, SAMRecord}
import org.apache.spark.broadcast.Broadcast
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.sequila.pileup.model.{ContigEventAggregate, ContigRange, PileupAccumulator, PileupRecord, PileupUpdate, TailEdge, UpdateStruct}
import org.biodatageeks.sequila.utils.DataQualityFuncs

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
  def calculatePileup(alignments: RDD[SAMRecord], spark: SparkSession): RDD[PileupRecord] = {

    val events = assembleEventsAggregates(alignments)
    val accumulator = accumulateTails(events, spark)
    val broadcast = spark.sparkContext.broadcast(prepareOverlaps(accumulator.value()))
    lazy val adjustedEvents = adjustEventAggregatesWithOverlaps(events, broadcast)
    eventsToPileup(adjustedEvents)
  }

  /**
    * Collects "interesting" (read start, stop, ref/nonref counting) events on alignments
    *
    * @param alignments aligned reads
    * @return distributed collection of PileupRecords
    */
  def assembleEventsAggregates(alignments: RDD[SAMRecord]): RDD[ContigEventAggregate] = {
    val contigLenMap = initContigLengths(alignments.first())

    alignments.mapPartitions { partition =>
      val aggMap = new mutable.HashMap[String, ContigEventAggregate]()
      val contigMaxReadLen = new mutable.HashMap[String, Int]()

      while (partition.hasNext) {
        val read = partition.next()
        val contig = DataQualityFuncs.cleanContig(read.getContig)

        if (!aggMap.contains(contig))
          handleFirstReadForContigInPartition(read, contig, contigLenMap, contigMaxReadLen, aggMap)

        val contigEventAggregate = aggMap(contig)
        analyzeRead(read, contig, contigEventAggregate, contigMaxReadLen)
      }
      lazy val output = prepareOutputAggregates(aggMap, contigMaxReadLen)
      output.toMap.values.iterator
    }

  }

  /**
    * gathers "tails" of events array that might be overlapping with other partitions. length of tail is equal to the
    * longest read in this aggregate
    * @param events events array with additional aggregate information from partition
    * @return
    */

  private def accumulateTails(events: RDD[ContigEventAggregate], spark:SparkSession): PileupAccumulator = {
    val accumulator = new PileupAccumulator(new PileupUpdate(new ArrayBuffer[TailEdge](), new ArrayBuffer[ContigRange]()))
    spark.sparkContext.register(accumulator, name="PileupAcc")

    events foreach {
      agg => {
        val contig = agg.contig
        val minPos = agg.startPosition
        val maxPos = agg.maxPosition
        val maxSeqLen = agg.maxSeqLen

        val tailStartPos = maxPos - maxSeqLen
        val tailCov = agg.events.takeRight(agg.maxSeqLen)

        val tail = TailEdge(contig, minPos, tailStartPos, tailCov, agg.events.sum)
        val range = ContigRange(contig, minPos, maxPos)
        val pileupUpdate = new PileupUpdate(ArrayBuffer(tail), ArrayBuffer(range))
        logger.debug(s"Adding record for: chr=$contig,start=$minPos,end=$maxPos,span=${maxPos-minPos+1},maxSeqLen=$maxSeqLen")
        accumulator.add(pileupUpdate)
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
    val updateMap = new mutable.HashMap[(String, Int), (Option[Array[Short]], Short)]()
    val shrinkMap = new mutable.HashMap[(String, Int), Int]()
    val minmax = new mutable.HashMap[String, (Int, Int)]()

    var it = 0
    for (range <- ranges.sortBy(r => (r.contig, r.minPos))) {
      if (!minmax.contains(range.contig))
        minmax += range.contig -> (Int.MaxValue, 0)

      val overlaps = findOverlappingTailsForRange(range, tails)
      val cumSum = precedingCumulativeSum(range, tails)

      if(overlaps.isEmpty)
        updateMap += (range.contig, range.minPos) -> (None, cumSum)
      else { // if there are  overlaps for this contigRange
        for(o <- overlaps) {
          val overlapLength = calculateOverlapLength(o, range, it, ranges)
          updateShrinksByOverlap(o, range, shrinkMap)
          updateUpdateByOverlap(o, overlapLength, range, cumSum, updateMap)
        }
        logger.debug(s"UpdateStruct len:${updateMap(range.contig,range.minPos)._1.get.length}, shrinkmap ${shrinkMap.mkString("|")}")
      }
      updateMinMaxByOverlap(range, minmax)
      it += 1
    }
    logger.debug(s"Prepared broadcast $updateMap, $shrinkMap")
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
    logger.debug("Updating aggregates. Agg count={}", events.count)

    val upd: mutable.HashMap[(String, Int), (Option[Array[Short]], Short)] = b.value.upd
    val shrink = b.value.shrink

    events map { agg => {
      logger.debug(
        s"Updating: $agg.contig, min=${agg.startPosition} max=${agg.maxPosition} len:${agg.events.length} span: ${agg.maxPosition - agg.startPosition} ")

      val updatedEventsArray = calculateEventsArrayWithBroadcast(agg, upd)
      val shrinkedEventsArray = shrinkEventsWithBroadcast(agg, shrink, updatedEventsArray)
      logger.debug(
        s"End of Update Partition: $agg.contig, min=${agg.startPosition} max=${agg.maxPosition} len:${agg.events.length} span: ${agg.maxPosition - agg.startPosition}")
      ContigEventAggregate(agg.contig, agg.contigLen, shrinkedEventsArray, agg.startPosition, agg.maxPosition)
    }}
  }



  @inline private def updateMinMaxByOverlap(range: ContigRange, minmax: mutable.HashMap[String, (Int, Int)]) = {
    if (range.minPos < minmax(range.contig)._1)
      minmax(range.contig) = (range.minPos, minmax(range.contig)._2)
    if (range.maxPos > minmax(range.contig)._2)
      minmax(range.contig) = (minmax(range.contig)._1, range.maxPos)
  }

  private def updateUpdateByOverlap(o: TailEdge, overlapLength: Int, range: ContigRange, cumSum: Short, updateMap: mutable.HashMap[(String, Int), (Option[Array[Short]], Short)]) = {
    updateMap.get((range.contig, range.minPos)) match {
      case Some(up) =>
        val arr = Array.fill[Short](math.max(0, o.startPoint - range.minPos))(0) ++ o.events.takeRight(overlapLength)
        val newArr = up._1.get.zipAll(arr, 0.toShort, 0.toShort).map { case (x, y) => (x + y).toShort }
        val newCumSum = (up._2 - o.events.takeRight(overlapLength).sum).toShort

        logger.debug(s"overlap o.minPos=${o.minPos} len = ${newArr.length}")
        logger.debug(s"next update to updateMap: range.minPos=${range.minPos}, o.minPos=${o.minPos} curr_len${up._1.get.length}, new_len = ${newArr.length}")

        if (o.minPos < range.minPos)
          updateMap.update((range.contig, range.minPos), (Some(newArr), newCumSum))
        else
          updateMap.update((range.contig, o.minPos), (Some(newArr), newCumSum)) // delete anything that is > range.minPos
      case _ =>
        logger.debug(s"overlap o.minPos=${o.minPos} o.max = ${o.minPos + overlapLength - 1} len = $overlapLength")
        logger.debug(s"first update to updateMap: ${math.max(0, o.startPoint - range.minPos)}, $overlapLength ")
        updateMap += (range.contig, range.minPos) -> (Some(Array.fill[Short](math.max(0, o.startPoint - range.minPos))(0) ++ o.events.takeRight(overlapLength)), (cumSum - o.events.takeRight(overlapLength).sum).toShort)
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
    logger.debug("Overlap length $l for $it from ${o.contig},${o.minPos}, ${o.startPoint},${o.cov.length}")
    length

  }

  @inline private def precedingCumulativeSum(range:ContigRange, tails: ArrayBuffer[TailEdge]): Short = tails
    .filter(t => t.contig == range.contig && t.minPos < range.minPos)
    .map(_.cumSum)
    .sum

  @inline private def findOverlappingTailsForRange(range: ContigRange, tails: ArrayBuffer[TailEdge]) = {
    tails
      .filter(t => (t.contig == range.contig && t.startPoint + t.events.length > range.minPos) && t.minPos < range.minPos)
      .sortBy(r => (r.contig, r.minPos))
  }


  /**
    * calculate maximum possible
    *
    * @param agg
    * @return
    */
  @inline
  def calculateMaxLength(agg: ContigEventAggregate, allPositions: Boolean): Int = {
    if (! allPositions)
       return agg.events.length

    val firstBlockMaxLength = agg.startPosition - 1
    val lastBlockMaxLength = agg.contigLen - agg.maxPosition
    firstBlockMaxLength + agg.events.length + lastBlockMaxLength

  }

  /**
    * Convert events array to pileup. Shows only non-zero coverage
    * Currently fixed constants added as REF, COUNTREF, COUNTNONREF
    * @param events events aggregates
    * @return rdd of pileup records
    */
  def eventsToPileup(events: RDD[ContigEventAggregate]): RDD[PileupRecord] = {

    events
      .mapPartitions { part =>
        part.map(agg => {
          var cov, ind, i = 0
          val allPos = false
          val result = new Array[PileupRecord](calculateMaxLength(agg, allPos))

          while (i < agg.events.length) {
            cov += agg.events(i)
            if (i != agg.events.length - 1) { //HACK. otherwise we get doubled CovRecords for partition boundary index
              if (allPos || cov != 0) { // show all positions or only non-zero
                result(ind) = PileupRecord(agg.contig, i + agg.startPosition, "A", cov.toShort, 10, 5)
                ind += 1
              }
            }
            i += 1
          }
          result.take(ind).iterator
        })
      }
      .flatMap(r => r)
  }



def calculateEventsArrayWithBroadcast(agg: ContigEventAggregate, upd: mutable.HashMap[(String, Int), (Option[Array[Short]], Short)]): Array[Short] = {
  var eventsArrMutable = agg.events

  upd.get((agg.contig, agg.startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
    case Some((arr, covSum)) => // array of covs and cumSum
      arr match {
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
          //logger.debug(s"$agg.contig, min=${agg.startPosition} max=${agg.maxPosition} Updated array ${eventsArrMutable.take(10).mkString(",")}")
          eventsArrMutable
        case None =>
          eventsArrMutable(0) = (eventsArrMutable(0) + covSum).toShort
          eventsArrMutable
      }
    case None =>
      eventsArrMutable
  }
}

  private def shrinkEventsWithBroadcast(agg: ContigEventAggregate, shrink: mutable.HashMap[(String, Int), Int], updArray: Array[Short]) = {
    shrink.get((agg.contig, agg.startPosition)) match {
      case Some(len) =>
        updArray.take(len)
      case None => updArray
    }
  }


  private def handleFirstReadForContigInPartition(read: SAMRecord, contig: String, contigLenMap: Map[String, Int], contigMaxReadLen: mutable.HashMap[String, Int], aggMap: mutable.HashMap[String, ContigEventAggregate]) = {
    val contigLen = contigLenMap(contig)
    val arrayLen = contigLen - read.getStart + 10

    val contigEventAggregate = ContigEventAggregate(
      contig = contig,
      contigLen = contigLen,
      events = new Array[Short](arrayLen),
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
    * @param eventAggregate   object holding current state of events aggregate in this contig
    * @param contigMaxReadLen map between contig and max read length (for overlaps)
    */
  def analyzeRead(read: SAMRecord, contig: String, eventAggregate: ContigEventAggregate, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {

    val partitionStart = eventAggregate.startPosition
    var position = read.getStart
    val cigarIterator = read.getCigar.iterator()
    var cigarLen = 0

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator

      if (cigarOperator == CigarOperator.M || cigarOperator == CigarOperator.X || cigarOperator == CigarOperator.EQ || cigarOperator == CigarOperator.N || cigarOperator == CigarOperator.D)
        cigarLen += cigarOperatorLen

      // update events array according to read alignment blocks start/end
      if (cigarOperator == CigarOperator.M || cigarOperator == CigarOperator.X || cigarOperator == CigarOperator.EQ) {
        updateContigEventsArray(position, partitionStart, contig, eventAggregate, delta = 1)
        position += cigarOperatorLen
        updateContigEventsArray(position, partitionStart, contig, eventAggregate, delta = -1)
      }
      else if (cigarOperator == CigarOperator.N || cigarOperator == CigarOperator.D)
        position += cigarOperatorLen
    }

    // seq len is not equal to cigar len (typically longer, because of clips, but the value is ready to use, doesn't nedd to be computed)
//    updateMaxReadLenInContig(read, contig, contigMaxReadLen)
    updateMaxCigarInContig(cigarLen, contig, contigMaxReadLen)

  }

  /**
    * transforms map structure of contigEventAggregates, by reducing number of last zeroes in the cov array
    * also adds calculated maxCigar len to output
    *
    * @param aggMap   mapper between contig and contigEventAggregate
    * @param cigarMap mapper between contig and max length of cigar in given
    * @return
    */
  def prepareOutputAggregates(aggMap: mutable.HashMap[String, ContigEventAggregate], cigarMap: mutable.HashMap[String, Int]): mutable.HashMap[String, ContigEventAggregate] =
    aggMap map (r => {
      val contig = r._1
      val contigEventAgg = r._2

      val maxIndex: Int = findMaxIndex(contigEventAgg.events)
      (contig, ContigEventAggregate(
        contig,
        contigEventAgg.contigLen,
        contigEventAgg.events.slice(0, maxIndex + 1),
        contigEventAgg.startPosition,
        contigEventAgg.startPosition + maxIndex,
        cigarMap(contig)))

    })

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
      logger.debug("sequence name {} => {} ", sequence.getSequenceName, sequence.getSequenceLength)
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

}
