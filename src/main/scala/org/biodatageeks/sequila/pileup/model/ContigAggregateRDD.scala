package org.biodatageeks.sequila.pileup.model

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.serializers.PileupProjection
import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.pileup.partitioning.PartitionBounds
import org.biodatageeks.sequila.utils.{DataQualityFuncs, FastMath}
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

      part.map { agg => {
        var cov, ind, i, currPos = 0
        val allPos = false
        val maxLen = agg.calculateMaxLength(allPos)
        val maxIndex = FastMath.findMaxIndex(agg.events)
        val result = new Array[InternalRow](maxLen)
        val prev = new BlockProperties()
        val startPosition = agg.startPosition
        val partitionBounds = bounds.value(index)
        val contigByte = UTF8String.fromString(agg.contig).getBytes
        val bases = reference.getBasesFromReference(DataQualityFuncs.unCleanContig(agg.contig), agg.startPosition, agg.startPosition + maxIndex)

        breakable {
          while (i <= maxIndex) {
            currPos = i + startPosition
            cov += agg.events(i)
            if (isInPartitionRange(currPos - 1 , agg.contig, partitionBounds, agg.conf)) {
              if (currPos == partitionBounds.postStart) {
                prev.reset(i)
              }
              if (prev.hasAlt) {
                addBaseRecord(result, ind, agg, contigByte, bases, i, prev)
                ind += 1;
                prev.reset(i)
                if (agg.hasAltOnPosition(currPos))
                  prev.alt = agg.alts(currPos)
              }
              else if (agg.hasAltOnPosition(currPos)) { // there is ALT in this posiion
                if (prev.isNonZeroCoverage) { // there is previous group non-zero group -> convert it
                  addBlockRecord(result, ind, agg, contigByte, bases, i, prev)
                  ind += 1;
                  prev.reset(i)
                } else if (prev.isZeroCoverage) // previous ZERO group, clear block
                  prev.reset(i)
                prev.alt = agg.alts(currPos)
              } else if (isEndOfZeroCoverageRegion(cov, prev.cov, i)) { // coming back from zero coverage. clear block
                prev.reset(i)
              } else if (isChangeOfCoverage(cov, prev.cov,  i) || isStartOfZeroCoverageRegion(cov, prev.cov)) { // different cov, add to output previous group
                addBlockRecord(result, ind, agg, contigByte, bases, i, prev)
                ind += 1;
                prev.reset(i)
              } else if (currPos == partitionBounds.posEnd + 1) { // last item -> convert it
                addBlockRecord(result, ind, agg, contigByte, bases, i, prev)
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


  def toCoverage(bounds: Broadcast[Array[PartitionBounds]] ) : RDD[InternalRow] = {

    this.rdd.mapPartitionsWithIndex { (index, part) =>
      part.map { agg => {
        val contigByte = UTF8String.fromString(agg.contig).getBytes

        var cov, ind, i, currPos = 0
        val allPos = false
        val maxLen = agg.calculateMaxLength(allPos)
        val maxIndex = FastMath.findMaxIndex(agg.events)
        val result = new Array[InternalRow](maxLen)
        val prev = new BlockProperties()
        val startPosition = agg.startPosition
        val partitionBounds = bounds.value(index)

        breakable {
          while (i <= maxIndex) {
            currPos = i + startPosition
            cov += agg.events(i)
            if (isInPartitionRange(currPos - 1 , agg.contig, partitionBounds, agg.conf)) {
              if (currPos == partitionBounds.postStart) {
                prev.reset(i)
              }
              if (isEndOfZeroCoverageRegion(cov, prev.cov, i)) { // coming back from zero coverage. clear block
                prev.reset(i)
              } else if (isChangeOfCoverage(cov, prev.cov,  i) || isStartOfZeroCoverageRegion(cov, prev.cov)) { // different cov, add to output previous group
                addBlockRecordNoRef(result, ind, agg, contigByte, i, prev)
                ind += 1;
                prev.reset(i)
              } else if (currPos == partitionBounds.posEnd + 1) { // last item -> convert it
                addBlockRecordNoRef(result, ind, agg, contigByte, i, prev)
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
                            agg:ContigAggregate, contigByte: Array[Byte], bases:String, i:Int, prev:BlockProperties): Unit = {
    val posStart, posEnd = i+agg.startPosition-1
    val ref = bases.substring(prev.pos, i)
    val altsCount = prev.alt.derivedAltsNumber
    val qualsMap = prepareOutputQualMap(agg, posStart, ref)
    result(ind) = PileupProjection.convertToRow(agg.contig, contigByte, posStart, posEnd, ref, prev.cov.toShort, (prev.cov-altsCount).toShort,altsCount.toShort, prev.alt, qualsMap)
    prev.alt.clear()
  }

  def updateQuals(inputBase: Byte, quality: Byte, ref: Char, isPositive: Boolean, map: mutable.IntMap[Array[Short]]):Unit = {
    val base = if (!isPositive && inputBase == ref) ref.toByte else if (!isPositive) inputBase.toChar.toLower.toByte else inputBase

    map.get(base) match {
      case None =>
        val arr =  new Array[Short](41)
        arr(quality) = 1
        map.put(base, arr)
      case Some(baseQuals) =>
        baseQuals(quality) = (baseQuals(quality) + 1).toShort
    }
  }

  def fillBaseQualities(rs: ReadSummary, altPos: Int, ref: Char, qualsMap: mutable.IntMap[Array[Short]]): Unit = {
    if (!rs.hasDeletionOnPosition(altPos)) {
      val relativePos = rs.relativePosition(altPos)
      val base = rs.bases(relativePos)
      updateQuals(base, rs.quals(relativePos), ref,  rs.isPositive, qualsMap)
    }
  }

  private def prepareOutputQualMap(agg: ContigAggregate, posStart: Int, ref:String): mutable.IntMap[Array[Short]] = {
    if (!agg.conf.includeBaseQualities)
      return null

    val newMap = mutable.IntMap[Array[Short]]()

    val rsIterator = agg.rsTree.overlappers(posStart, posStart)
    while (rsIterator.hasNext) {
      val nodeIterator = rsIterator.next().getValue.iterator()
      while (nodeIterator.hasNext) {
        fillBaseQualities(nodeIterator.next(), posStart, ref(0), newMap)
      }
    }
    newMap
  }

  private def addBlockRecord(result:Array[InternalRow], ind:Int,
                             agg:ContigAggregate, contigByte: Array[Byte], bases:String, i:Int, prev:BlockProperties): Unit = {
    val ref = bases.substring(prev.pos, i)
    val posStart=i+agg.startPosition-prev.len
    val posEnd=i+agg.startPosition-1
    result(ind) = PileupProjection.convertToRow(agg.contig, contigByte, posStart, posEnd, ref, prev.cov.toShort, prev.cov.toShort, 0.toShort,null,null )
  }
  private def addBlockRecordNoRef(result:Array[InternalRow], ind:Int,
                             agg:ContigAggregate, contigByte: Array[Byte], i:Int, prev:BlockProperties): Unit = {
    val ref = "R"*(i-prev.pos)
    val posStart=i+agg.startPosition-prev.len
    val posEnd=i+agg.startPosition-1
    result(ind) = PileupProjection.convertToRow(agg.contig, contigByte, posStart, posEnd, ref, prev.cov.toShort, prev.cov.toShort, 0.toShort,null,null )
  }
}
