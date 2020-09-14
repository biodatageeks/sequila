package org.biodatageeks.sequila.pileup.model

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.sequila.pileup.broadcast.{FullCorrections, PileupAccumulator, PileupUpdate, Range, Tail}
import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.pileup.serializers.PileupProjection
import org.biodatageeks.sequila.pileup.timers.PileupTimers.{AccumulatorAddTimer, AccumulatorAllocTimer, AccumulatorNestedTimer, AccumulatorRegisterTimer, PileupUpdateCreationTimer}
import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.pileup.model.Quals._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object AggregateRDDOperations {
  object implicits {
    implicit def aggregateRdd(rdd: RDD[ContigAggregate]): AggregateRDD = AggregateRDD(rdd)
  }
}

case class AggregateRDD(rdd: RDD[ContigAggregate]) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  /**
    * gathers "tails" of events array that might be overlapping with other partitions. length of tail is equal to the
    * longest read in this aggregate
    * @return
    */

  def accumulateTails( spark:SparkSession): PileupAccumulator = {
    val accumulator = AccumulatorAllocTimer.time {new PileupAccumulator(new PileupUpdate(new ArrayBuffer[Tail](), new ArrayBuffer[Range]())) }
    AccumulatorRegisterTimer.time {spark.sparkContext.register(accumulator) }

    this.rdd foreach {
      agg => {AccumulatorNestedTimer.time {
        val pu = PileupUpdateCreationTimer.time {agg.getPileupUpdate}
        AccumulatorAddTimer.time {accumulator.add(pu)}}}
    }
    accumulator
  }

  def adjustWithOverlaps(b: Broadcast[FullCorrections])
  : RDD[ContigAggregate] = {
    this.rdd map { agg =>  agg.getAdjustedAggregate(b)}
  }

  def toPileup(refPath: String ) : RDD[InternalRow] = {

    this.rdd.mapPartitions { part =>
      val reference = new Reference(refPath)
      val contigMap = reference.getNormalizedContigMap
      PileupProjection.setContigMap(contigMap)

      part.map { agg => {
        var cov, ind, i = 0
        val allPos = false
        val maxLen = agg.calculateMaxLength(allPos)
        val result = new Array[InternalRow](maxLen)
        val prev = new BlockProperties()
        val startPosition = agg.startPosition
        val bases = reference.getBasesFromReference(contigMap(agg.contig), agg.startPosition, agg.startPosition + agg.events.length - 1)

        while (i < agg.shrinkedEventsArraySize) {
          cov += agg.events(i)
          if (prev.hasAlt) {
            addBaseRecord(result, ind, agg, bases, i, prev)
            ind += 1;
            prev.reset(i)
            if (agg.hasAltOnPosition(i+startPosition))
              prev.alt=agg.alts(i+startPosition)
          }
          else if (agg.hasAltOnPosition(i+startPosition)) { // there is ALT in this posiion
            if (prev.isNonZeroCoverage) { // there is previous group non-zero group -> convert it
              addBlockRecord(result, ind, agg, bases, i, prev)
              ind += 1;
              prev.reset(i)
            } else if (prev.isZeroCoverage) // previous ZERO group, clear block
            prev.reset(i)
            prev.alt = agg.alts(i+startPosition)
          } else if (isEndOfZeroCoverageRegion(cov, prev.cov, i)) { // coming back from zero coverage. clear block
            prev.reset(i)
          } else if (isChangeOfCoverage(cov, prev.cov, i) || isStartOfZeroCoverageRegion(cov, prev.cov)) { // different cov, add to output previous group
            addBlockRecord(result, ind, agg, bases, i, prev)
            ind += 1;
            prev.reset(i)
          }  else if (i==agg.shrinkedEventsArraySize-1) { // last item -> convert it
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

  private def isStartOfZeroCoverageRegion(cov: Int, prevCov: Int) = cov == 0 && prevCov > 0
  private def isChangeOfCoverage(cov: Int, prevCov: Int, i: Int) = cov != 0 && prevCov >= 0 && prevCov != cov && i > 0
  private def isEndOfZeroCoverageRegion(cov: Int, prevCov: Int, i: Int) = cov != 0 && prevCov == 0 && i > 0

  private def addBaseRecord(result:Array[InternalRow], ind:Int,
                    agg:ContigAggregate, bases:String, i:Int, prev:BlockProperties) {
    val posStart, posEnd = i+agg.startPosition-1
    val ref = bases.substring(prev.pos, i)
    val altsCount = prev.alt.derivedAltsNumber
    val qualsMap = prepareOutputQualMap(agg, posStart, ref, prev.cov.toShort)
    result(ind) = PileupProjection.convertToRow(agg.contig, posStart, posEnd, ref, prev.cov.toShort, (prev.cov-altsCount).toShort,altsCount, prev.alt.toMap, qualsMap)
    prev.alt.clear()
  }

  private def prepareOutputQualMap(agg: ContigAggregate, posStart: Int, ref:String, cov: Short): Map[Byte, Array[Short]] = {
    if (!Conf.includeBaseQualities)
      return null

    val qualsMap = agg.quals(posStart)
    qualsMap.zipWithIndex.map { case (_, i) =>
      val ind = i + QualityConstants.QUAL_INDEX_SHIFT
      if (qualsMap(i) != null)
        (if(ind != QualityConstants.REF_SYMBOL) ind.toByte else ref(0).toByte) -> qualsMap(i)
      else null
    }.filter(_ != null).toMap
  }

  private def addBlockRecord(result:Array[InternalRow], ind:Int,
                             agg:ContigAggregate, bases:String, i:Int, prev:BlockProperties) {
    val ref = bases.substring(prev.pos, i)
    val posStart=i+agg.startPosition-prev.len
    val posEnd=i+agg.startPosition-1
    result(ind) = PileupProjection.convertToRow(agg.contig, posStart, posEnd, ref, prev.cov.toShort, prev.cov.toShort, 0.toShort,null,null )
  }
}
