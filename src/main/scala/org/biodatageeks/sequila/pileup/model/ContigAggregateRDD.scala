package org.biodatageeks.sequila.pileup.model

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ListColumnVector, LongColumnVector, MapColumnVector, VectorizedRowBatch}
import org.apache.hadoop.io.{IntWritable, NullWritable, ShortWritable, Text}
import org.apache.orc.{OrcFile, TypeDescription, Writer}
import org.apache.orc.mapred.OrcStruct
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.unsafe.types.UTF8String
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.serializers.{OrcProjection, PileupProjection}
import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.pileup.partitioning.PartitionBounds
import org.biodatageeks.sequila.utils.{AlignmentConstants, Columns, FastMath}
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


  def toPileupVectorizedWriter(refPath: String, bounds: Broadcast[Array[PartitionBounds]],  output: Seq[Attribute], conf: Broadcast[Conf] ) : RDD[Int] = {

    logger.info(s"### Pileup: using Vectorized ORC output optimization with ${conf.value.orcCompressCodec} compression")
    this.rdd.mapPartitionsWithIndex { (index, part) =>
      val reference = new Reference(refPath)
      val contigMap = reference.getNormalizedContigMap
      PileupProjection.setContigMap(contigMap)

      part.map { agg => {
        var cov, ind, i, currPos = 0
        val allPos = false
        val maxIndex = FastMath.findMaxIndex(agg.events)
        val result = 0
        val prev = new BlockProperties()
        val startPosition = agg.startPosition
        val partitionBounds = bounds.value(index)
        val bases = reference.getBasesFromReference(contigMap(agg.contig), agg.startPosition, agg.startPosition + maxIndex)

        val hadoopConf = new Configuration()
        hadoopConf.set("orc.compress", conf.value.orcCompressCodec )
        val path = conf.value.vectorizedOrcWriterPath
        val schema = OrcProjection.catalystToOrcSchema(output)
        val writer = OrcFile
          .createWriter(new Path(s"${path}/part-${index}-${System.currentTimeMillis()}.${conf.value.orcCompressCodec.toLowerCase}.orc"),
            OrcFile.writerOptions(hadoopConf).setSchema(schema))
        val batch = schema.createRowBatch
        val contigVector = batch.cols(0).asInstanceOf[BytesColumnVector]
        val postStartVector = batch.cols(1).asInstanceOf[LongColumnVector]
        val postEndVector = batch.cols(2).asInstanceOf[LongColumnVector]
        val refVector = batch.cols(3).asInstanceOf[BytesColumnVector]
        val covVector = batch.cols(4).asInstanceOf[LongColumnVector]
        val countRefVector = batch.cols(5).asInstanceOf[LongColumnVector]
        val countNonRefVector = batch.cols(6).asInstanceOf[LongColumnVector]
        //alts
        val altsMapVector = batch.cols(7).asInstanceOf[MapColumnVector]
        val altsMapKey = altsMapVector.keys.asInstanceOf[LongColumnVector]
        val altsMapValue = altsMapVector.values.asInstanceOf[LongColumnVector]
        altsMapVector.noNulls = false
        val ALTS_MAP_SIZE = 10
        val BATCH_SIZE = batch.getMaxSize
        altsMapKey.ensureSize(BATCH_SIZE * ALTS_MAP_SIZE, false)
        altsMapValue.ensureSize(BATCH_SIZE * ALTS_MAP_SIZE, false)

        var row: Int = 0
        breakable {
          while (i <= maxIndex) {
            row = batch.size
            currPos = i + startPosition
            cov += agg.events(i)
            if (isInPartitionRange(currPos - 1 , agg.contig, partitionBounds, agg.conf)) {
              if (currPos == partitionBounds.postStart) {
                prev.reset(i)
              }
              if (prev.hasAlt) {
                addBaseRecordVectorizedWriterOrc(ind, agg, bases, i, prev,
                  contigVector,
                  postStartVector,
                  postEndVector,
                  refVector,
                  covVector,
                  countRefVector,
                  countNonRefVector,
                  altsMapVector, writer, batch, row)
                ind += 1;
                prev.reset(i)
                if (agg.hasAltOnPosition(currPos))
                  prev.alt = agg.alts(currPos)
              }
              else if (agg.hasAltOnPosition(currPos)) { // there is ALT in this posiion
                if (prev.isNonZeroCoverage) { // there is previous group non-zero group -> convert it
                  addBlockVectorizedWriterOrc(ind, agg, bases, i, prev,
                    contigVector, postStartVector, postEndVector, refVector, covVector,
                    countRefVector,
                    countNonRefVector,
                    altsMapVector, writer, batch, row
                  )
                  ind += 1;
                  prev.reset(i)
                } else if (prev.isZeroCoverage) // previous ZERO group, clear block
                  prev.reset(i)
                prev.alt = agg.alts(currPos)
              } else if (isEndOfZeroCoverageRegion(cov, prev.cov, i)) { // coming back from zero coverage. clear block
                prev.reset(i)
              } else if (isChangeOfCoverage(cov, prev.cov,  i) || isStartOfZeroCoverageRegion(cov, prev.cov)) { // different cov, add to output previous group
                addBlockVectorizedWriterOrc(ind, agg, bases, i, prev,
                  contigVector, postStartVector, postEndVector, refVector, covVector,
                  countRefVector,
                  countNonRefVector,
                  altsMapVector,  writer, batch, row
                )
                ind += 1;
                prev.reset(i)
              } else if (currPos == partitionBounds.posEnd + 1) { // last item -> convert it
                addBlockVectorizedWriterOrc(ind, agg, bases, i, prev,
                  contigVector, postStartVector, postEndVector, refVector, covVector,
                  countRefVector,
                  countNonRefVector,
                  altsMapVector, writer, batch, row
                )
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
        if (batch.size != 0) {
          writer.addRowBatch(batch)
          batch.reset
        }
        writer.close
        result
      }
      }
    }
  }



  def toCoverage(bounds: Broadcast[Array[PartitionBounds]] ) : RDD[InternalRow] = {

    this.rdd.mapPartitionsWithIndex { (index, part) =>

      part.map { agg => {
        val contigMap = new mutable.HashMap[String, Array[Byte]]()
        contigMap += (agg.contig -> UTF8String.fromString(agg.contig).getBytes)
        PileupProjection.contigByteMap=contigMap

        var cov, ind, i, currPos = 0
        val allPos = false
        val maxLen = agg.calculateMaxLength(allPos)
        val maxIndex = FastMath.findMaxIndex(agg.events)
        val result = new Array[InternalRow](maxLen)
        val prev = new BlockProperties()
        val startPosition = agg.startPosition
        val partitionBounds = bounds.value(index)
        val bases = AlignmentConstants.REF_SYMBOL

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


  def toCoverageVectorizedWriter(bounds: Broadcast[Array[PartitionBounds]], output: Seq[Attribute], conf: Broadcast[Conf]) : RDD[Int] = {

    logger.info(s"### Coverage: using Vectorized ORC output optimization with ${conf.value.orcCompressCodec} compression")
    this.rdd.mapPartitionsWithIndex { (index, part) =>

      part.map { agg => {
        val contigMap = new mutable.HashMap[String, Array[Byte]]()
        contigMap += (agg.contig -> UTF8String.fromString(agg.contig).getBytes)
        PileupProjection.contigByteMap=contigMap

        var cov, ind, i, currPos = 0
        val maxIndex = FastMath.findMaxIndex(agg.events)
        val result = 0
        val prev = new BlockProperties()
        val startPosition = agg.startPosition
        val partitionBounds = bounds.value(index)
        val bases = AlignmentConstants.REF_SYMBOL

        val hadoopConf = new Configuration()
        hadoopConf.set("orc.compress", conf.value.orcCompressCodec )
        val path = conf.value.vectorizedOrcWriterPath
        val schema = OrcProjection.catalystToOrcSchema(output)
        val writer = OrcFile
          .createWriter(new Path(s"${path}/part-${index}-${System.currentTimeMillis()}.${conf.value.orcCompressCodec.toLowerCase}.orc"),
          OrcFile.writerOptions(hadoopConf).setSchema(schema))
        val batch = schema.createRowBatch
        val contigVector = batch.cols(0).asInstanceOf[BytesColumnVector]
        val postStartVector = batch.cols(1).asInstanceOf[LongColumnVector]
        val postEndVector = batch.cols(2).asInstanceOf[LongColumnVector]
        val refVector = batch.cols(3).asInstanceOf[BytesColumnVector]
        val covVector = batch.cols(4).asInstanceOf[LongColumnVector]
        var row: Int = 0

        breakable {
          while (i <= maxIndex) {
            row = batch.size
            currPos = i + startPosition
            cov += agg.events(i)
            if (isInPartitionRange(currPos - 1 , agg.contig, partitionBounds, agg.conf)) {
              if (currPos == partitionBounds.postStart) {
                prev.reset(i)
              }
              if (isEndOfZeroCoverageRegion(cov, prev.cov, i)) { // coming back from zero coverage. clear block
                prev.reset(i)
              } else if (isChangeOfCoverage(cov, prev.cov,  i) || isStartOfZeroCoverageRegion(cov, prev.cov)) { // different cov, add to output previous group
                addBlockVectorizedWriterOrc(ind, agg, bases, i, prev,
                  contigVector, postStartVector, postEndVector, refVector, covVector,
                  null,
                  null,
                  null,
                   writer, batch, row
                )
                ind += 1;
                prev.reset(i)
              } else if (currPos == partitionBounds.posEnd + 1) { // last item -> convert it
                addBlockVectorizedWriterOrc(ind, agg, bases, i, prev,
                  contigVector, postStartVector, postEndVector, refVector, covVector,
                  null,
                  null,
                  null,
                  writer, batch, row
                  )
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
        if (batch.size != 0) {
          writer.addRowBatch(batch)
          batch.reset
        }
        writer.close
        result
      }
      }
    }
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
    result(ind) = PileupProjection.convertToRow(agg.contig,
      posStart, posEnd, ref, prev.cov.toShort,
      (prev.cov-altsCount).toShort,altsCount.toShort, prev.alt, qualsMap, agg.conf)
    prev.alt.clear()
  }

  private def addBaseRecordVectorizedWriterOrc(ind:Int,
                            agg:ContigAggregate, bases:String, i:Int, prev:BlockProperties,
                            contigVector: BytesColumnVector,
                            postStartVector: LongColumnVector,
                            postEndVector: LongColumnVector,
                            refVector: BytesColumnVector,
                            covVector: LongColumnVector,
                            countRefVector: LongColumnVector = null,
                            countNonRefVector: LongColumnVector = null,
                            altsMapVector: MapColumnVector = null,
                            writer: Writer,
                            batch: VectorizedRowBatch,
                            row: Int,
                            altsMapSize: Int = 10
                                              ): Unit = {
    val posStart, posEnd = i+agg.startPosition-1
    val ref = bases.substring(prev.pos, i)
    val altsCount = prev.alt.derivedAltsNumber
    val qualsMap = prepareOutputQualMap(agg, posStart, ref)
    contigVector.setVal(row, agg.contig.getBytes)
    postStartVector.vector(row) = posStart
    postEndVector.vector(row) = posEnd
    refVector.setVal(row, ref.getBytes())
    covVector.vector(row) = prev.cov.toShort
    countRefVector.vector(row) = (prev.cov-altsCount).toShort
    countNonRefVector.vector(row) = altsCount.toShort
    altsMapVector.offsets(row) = altsMapVector.childCount
    altsMapVector.lengths(row) = altsMapSize
    altsMapVector.childCount  += altsMapSize
    val mapOffset = altsMapVector.offsets(row).toInt
    var mapElem: Int = mapOffset
    val alts = prev.alt.toIterator
    while(mapElem < mapOffset + altsMapSize && alts.hasNext){
      val alt = alts.next()
      altsMapVector.keys.asInstanceOf[LongColumnVector].vector(mapElem) = alt._1
      altsMapVector.values.asInstanceOf[LongColumnVector].vector(mapElem) = alt._2
      mapElem += 1
    }


    batch.size += 1

    if (batch.size == batch.getMaxSize) {
      writer.addRowBatch(batch)
      batch.reset
    }

//    result(ind) = PileupProjection.convertToRow(agg.contig,
//      posStart, posEnd, ref, prev.cov.toShort,
//      (prev.cov-altsCount).toShort,altsCount.toShort, prev.alt, qualsMap, agg.conf)
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
                             agg:ContigAggregate, bases:String, i:Int, prev:BlockProperties): Unit = {
    val ref = if(agg.conf.coverageOnly) "R" else bases.substring(prev.pos, i)
    val posStart=i+agg.startPosition-prev.len
    val posEnd=i+agg.startPosition-1
    result(ind) = PileupProjection.convertToRow(agg.contig, posStart,
      posEnd, ref, prev.cov.toShort, prev.cov.toShort, 0.toShort,null, null, agg.conf)
  }

  private def addBlockVectorizedWriterOrc(ind:Int,
                         agg:ContigAggregate,
                         bases:String, i:Int, prev:BlockProperties,
                         contigVector: BytesColumnVector,
                         postStartVector: LongColumnVector,
                         postEndVector: LongColumnVector,
                         refVector: BytesColumnVector,
                         covVector: LongColumnVector,
                         countRefVector: LongColumnVector = null,
                         countNonRefVector: LongColumnVector = null,
                         altsMapVector: MapColumnVector = null,
                         writer: Writer,
                         batch: VectorizedRowBatch, row: Int, altsMapSize: Int = 10): Unit = {

    val ref = if(agg.conf.coverageOnly) "R" else bases.substring(prev.pos, i)
    val posStart=i+agg.startPosition-prev.len
    val posEnd=i+agg.startPosition-1
    contigVector.setVal(row, agg.contig.getBytes)
    postStartVector.vector(row) = posStart
    postEndVector.vector(row) = posEnd
    refVector.setVal(row, ref.getBytes())
    covVector.vector(row) = prev.cov.toShort

    if(!agg.conf.coverageOnly) {
      countRefVector.vector(row) = prev.cov.toShort
      countNonRefVector.vector(row) = 0.toShort
      altsMapVector.isNull(row) = true
    }
    batch.size += 1

    if (batch.size == batch.getMaxSize) {
      writer.addRowBatch(batch)
      batch.reset
    }
  }
}
