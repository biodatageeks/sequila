package org.biodatageeks.sequila.pileup.model

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.pileup.serializers.PileupProjection
import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.pileup.model.Quals._
import org.biodatageeks.sequila.pileup.model.Quals.SingleLocusQuals
import org.biodatageeks.sequila.pileup.partitioning.PartitionBounds
import org.biodatageeks.sequila.utils.FastMath
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.control.Breaks._

object AggregateRDDOperations {
  object implicits {
    implicit def aggregateRdd(rdd: RDD[ContigAggregate]): AggregateRDD = AggregateRDD(rdd)
  }
}

case class AggregateRDD(rdd: RDD[ContigAggregate]) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  def isInPartitionRange(currPos: Int, contig: String, bound: PartitionBounds,conf:Conf): Boolean = {
    if (contig == bound.contigStart && contig == bound.contigEnd)
      currPos >= bound.postStart - 1 && currPos <= bound.posEnd
    else if (contig == bound.contigStart && bound.contigEnd == conf.unknownContigName)
      currPos >= bound.postStart - 1
    else if (contig != bound.contigStart && bound.contigEnd == conf.unknownContigName)
      true
    else
      false
  }

  def toPileup(refPath: String, bounds: Broadcast[Array[PartitionBounds]] ) : RDD[InternalRow] = {

    this.rdd.mapPartitionsWithIndex { (index, part) =>
      val reference = new Reference(refPath)
      val contigMap = reference.getNormalizedContigMap
      PileupProjection.setContigMap(contigMap)

      part.map { agg => {
        var cov, ind, i, currPos = 0
        val allPos = false
        val maxLen = agg.calculateMaxLength(allPos)
        val maxIndex = FastMath.findMaxIndex(agg.events)
        val result = new Array[InternalRow](maxLen)
        val prev = new BlockProperties()
        val startPosition = agg.startPosition
        val partitionBounds = bounds.value(index)
        val bases = reference.getBasesFromReference(contigMap(agg.contig), agg.startPosition, agg.startPosition + maxIndex)

        breakable {
          while (i <= maxIndex) {
            currPos = i + startPosition
            cov += agg.events(i)
            if (isInPartitionRange(currPos - 1 , agg.contig, partitionBounds, agg.conf)) {
              if (currPos == partitionBounds.postStart) {
                prev.reset(i)
              }
              if (prev.hasAlt) {
                addBaseRecord(result, ind, agg, bases, i, prev)
                ind += 1;
                prev.reset(i)
                if (agg.hasAltOnPosition(currPos))
                  prev.alt = agg.alts(currPos)
              }
              else if (agg.hasAltOnPosition(currPos)) { // there is ALT in this posiion
                if (prev.isNonZeroCoverage) { // there is previous group non-zero group -> convert it
                  addBlockRecord(result, ind, agg, bases, i, prev)
                  ind += 1;
                  prev.reset(i)
                } else if (prev.isZeroCoverage) // previous ZERO group, clear block
                  prev.reset(i)
                prev.alt = agg.alts(currPos)
              } else if (isEndOfZeroCoverageRegion(cov, prev.cov, i)) { // coming back from zero coverage. clear block
                prev.reset(i)
              } else if (isChangeOfCoverage(cov, prev.cov,  i) || isStartOfZeroCoverageRegion(cov, prev.cov)) { // different cov, add to output previous group
                addBlockRecord(result, ind, agg, bases, i, prev)
                ind += 1;
                prev.reset(i)
              } else if (currPos == partitionBounds.posEnd + 1) { // last item -> convert it
                addBlockRecord(result, ind, agg, bases, i, prev)
                ind += 1;
                prev.reset(i)
                break
              }
            }
            prev.cov = cov;
            prev.len = prev.len + 1;
            i += 1
          } // while
        }
        result.take(ind)
      }
      }
    }.flatMap(r => r)
  }

  private def isStartOfZeroCoverageRegion(cov: Int, prevCov: Int) = cov == 0 && prevCov > 0
  private def isChangeOfCoverage(cov: Int, prevCov: Int, i: Int) = cov != 0 && prevCov >= 0 && prevCov != cov && i > 0
  private def isEndOfZeroCoverageRegion(cov: Int, prevCov: Int, i: Int) = cov != 0 && prevCov == 0 && i > 0

  private def addBaseRecord(result:Array[InternalRow], ind:Int,
                            agg:ContigAggregate, bases:String, i:Int, prev:BlockProperties): Unit = {
    val posStart, posEnd = i+agg.startPosition-1
    val ref = bases.substring(prev.pos, i)
    val altsCount = prev.alt.derivedAltsNumber
    val qualsMap = prepareOutputQualMap(agg, posStart, ref)
    result(ind) = PileupProjection.convertToRow(agg.contig, posStart, posEnd, ref, prev.cov.toShort, (prev.cov-altsCount).toShort,altsCount, prev.alt, qualsMap)
    prev.alt.clear()
  }

  private def mergeArrays(arr1: Array[Short], arr2: Array[Short]): Array[Short] = {
    if (arr2 == null) arr1
    else arr1.zip(arr2).map { case (x, y) => (x + y).toShort }
  }

  private def prepareOutputQualMap(agg: ContigAggregate, posStart: Int, ref:String): mutable.HashMap[Byte, Array[Short]] = {
    if (!agg.conf.includeBaseQualities)
      return null

    val qualsMap = agg.quals(posStart)
    val newMap = mutable.HashMap[Byte, Array[Short]]()

    for (i <- qualsMap.indices){
      if (qualsMap(i) != null ) {
        val ind = Quals.mapIdxToBase(i)
        val isZeros = qualsMap.isAllZeros(i)
        if (!isZeros) {
          if (ind == ref(0)) {
            val i_lower = Quals.mapBaseToIdx(ind.toChar.toLower)
            qualsMap(i) = mergeArrays(qualsMap(i), qualsMap(i_lower))
            qualsMap(i_lower) = null
            newMap(ind) = qualsMap(i)
          } else if (ind == ref(0).toLower) {
            newMap(ind.toChar.toUpper.toByte) = qualsMap(i)
          } else
            newMap(ind) = qualsMap(i)
        }
      }
    }
    newMap
  }

  private def addBlockRecord(result:Array[InternalRow], ind:Int,
                             agg:ContigAggregate, bases:String, i:Int, prev:BlockProperties) {
    val ref = bases.substring(prev.pos, i)
    val posStart=i+agg.startPosition-prev.len
    val posEnd=i+agg.startPosition-1
    result(ind) = PileupProjection.convertToRow(agg.contig, posStart, posEnd, ref, prev.cov.toShort, prev.cov.toShort, 0.toShort,null,null )
  }
}
