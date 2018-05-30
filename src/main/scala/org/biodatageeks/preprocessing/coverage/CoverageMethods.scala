package org.biodatageeks.preprocessing.coverage

import htsjdk.samtools.{Cigar, CigarOperator, SAMUtils, TextCigarCodec}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.datasources.BAM.BAMRecord
import org.biodatageeks.preprocessing.coverage.CoverageHistType.CoverageHistType
import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class CoverageRecord(sampleId:String,
                              chr:String,
                              position:Int,
                              coverage:Int)

case class CoverageRecordHist(sampleId:String,
                                  chr:String,
                                  position:Int,
                                  coverage:Array[Int],
                                  coverageTotal:Int)

case class PartitionCoverage(covMap: mutable.HashMap[(String, Int), Array[Int]],
                             maxCigarLength: Int,
                             outputSize: Int,
                             chrMinMax: Array[(String,Int)] )

case class PartitionCoverageHist(covMap: mutable.HashMap[(String, Int), (Array[Array[Int]],Array[Int])],
                                 maxCigarLength: Int,
                                 outputSize: Int,
                                 chrMinMax: Array[(String,Int)]
                                )
object CoverageHistType extends Enumeration{
  type CoverageHistType = Value
  val MAPQ = Value
}
case class CoverageHistParam(
                              histType : CoverageHistType,
                              buckets: Array[Double]
                            )

class CoverageReadFunctions(covReadRDD:RDD[BAMRecord]) extends Serializable {

  def baseCoverage(minMapq: Option[Int], numTasks: Option[Int] = None, sorted: Boolean):RDD[CoverageRecord] ={
    val sampleId = covReadRDD
      .first()
      .sampleId
    lazy val cov =numTasks match {
        case Some(n) => covReadRDD.repartition(n)
        case _ => covReadRDD
      }

      lazy val covQual = minMapq match {
        case Some(qual) => cov //FIXME add filtering
        case _ => cov
      }
        lazy val partCov = {
          sorted match {
            case true => covQual//.instrument()
            case _ => covQual.sortBy(r => (r.contigName, r.start))
          }
        }.mapPartitions { partIterator =>
          val covMap = new mutable.HashMap[(String, Int), Array[Int]]()
          val numSubArrays = 10000
          val subArraySize = 250000000 / numSubArrays
          val chrMinMax = new ArrayBuffer[(String, Int)]()
          var maxCigarLength = 0
          var lastChr = "NA"
          var lastPosition = 0
          var outputSize = 0

          for (cr <- partIterator) {
            val cigar = TextCigarCodec.decode(cr.cigar)
            val cigIterator = cigar.iterator()
            var position = cr.start
            val cigarStartPosition = position
            while (cigIterator.hasNext) {
              val cigarElement = cigIterator.next()
              val cigarOpLength = cigarElement.getLength
              val cigarOp = cigarElement.getOperator
              if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
                var currPosition = 0
                while (currPosition < cigarOpLength) {
                  val subIndex = position % numSubArrays
                  val index = position / numSubArrays

                  if (!covMap.keySet.contains(cr.contigName, index)) {
                    covMap += (cr.contigName, index) -> Array.fill[Int](subArraySize)(0)
                  }
                  covMap(cr.contigName, index)(subIndex) += 1
                  position += 1
                  currPosition += 1

                  /*add first*/
                  if (outputSize == 0) chrMinMax.append((cr.contigName, position))
                  if (covMap(cr.contigName, index)(subIndex) == 1) outputSize += 1

                }
              }
              else if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D) position += cigarOpLength
            }
            val currLength = position - cigarStartPosition
            if (maxCigarLength < currLength) maxCigarLength = currLength
            lastPosition = position
            lastChr = cr.contigName
          }
          chrMinMax.append((lastChr, lastPosition))
          Array(PartitionCoverage(covMap, maxCigarLength, outputSize, chrMinMax.toArray)).iterator
        }.persist(StorageLevel.MEMORY_AND_DISK_SER)
        val maxCigarLengthGlobal = partCov.map(r => r.maxCigarLength).reduce((a, b) => scala.math.max(a, b))
        lazy val combOutput = partCov.mapPartitions { partIterator =>
          /*split for reduction basing on position and max cigar length across all partitions - for gap alignments*/
          val partitionCoverageArray = (partIterator.toArray)
          val partitionCoverage = partitionCoverageArray(0)
          val chrMinMax = partitionCoverage.chrMinMax
          lazy val output = new Array[Array[CoverageRecord]](2)
          lazy val outputArray = new Array[CoverageRecord](partitionCoverage.outputSize)
          lazy val outputArraytoReduce = new ArrayBuffer[CoverageRecord]()
          val covMap = partitionCoverage.covMap
          var cnt = 0
          for (key <- covMap.keys) {
            var locCnt = 0
            for (value <- covMap.get(key).get) {
              if (value > 0) {
                val position = key._2 * 10000 + locCnt
                if (key._1 == chrMinMax.head._1 && position <= chrMinMax.head._2 + maxCigarLengthGlobal ||
                  key._1 == chrMinMax.last._1 && position >= chrMinMax.last._2 - maxCigarLengthGlobal)
                  outputArraytoReduce.append(CoverageRecord(sampleId,key._1, position, value))
                else
                  outputArray(cnt) = (CoverageRecord(sampleId,key._1, position, value))
                cnt += 1
              }
              locCnt += 1
            }
          } /*only records from the beginning and end of the partition for reduction the rest pass-through */
          output(0) = outputArray.filter(r => r != null)
          output(1) = outputArraytoReduce.toArray
          Iterator(output)
        }
        //partCov.unpersist()
        lazy val covReduced = combOutput.flatMap(r => r.array(1)).map(r => ((r.chr, r.position), r))
          .reduceByKey((a, b) => CoverageRecord(sampleId,a.chr, a.position, a.coverage + b.coverage))
          .map(_._2)
        partCov.unpersist()
        combOutput.flatMap(r => (r.array(0)))
          .union(covReduced)

  }
  def baseCoverageHist(minMapq: Option[Int], numTasks: Option[Int] = None, coverageHistParam: CoverageHistParam) /*: RDD[CoverageRecordSlimHist]*/ = {
    val sampleId = covReadRDD
      .first()
      .sampleId
    lazy val cov =numTasks match {
      case Some(n) => covReadRDD.repartition(n)
      case _ => covReadRDD
    }

    lazy val covQual = minMapq match {
      case Some(qual) => cov //FIXME add filtering
      case _ => cov
    }
    lazy val partCov = covQual
      .sortBy(r => (r.contigName, r.start))
      .mapPartitions { partIterator =>
        val covMap = new mutable.HashMap[(String, Int), (Array[Array[Int]], Array[Int])]()
        val numSubArrays = 10000
        val subArraySize = 250000000 / numSubArrays
        val chrMinMax = new ArrayBuffer[(String, Int)]()
        var maxCigarLength = 0
        var lastChr = "NA"
        var lastPosition = 0
        var outputSize = 0
        for (cr <- partIterator) {
          val cigar = TextCigarCodec.decode(cr.cigar)
          val cigInterator = cigar.getCigarElements.iterator()
          var position = cr.start
          val cigarStartPosition = position
          while (cigInterator.hasNext) {
            val cigarElement = cigInterator.next()
            val cigarOpLength = cigarElement.getLength
            val cigarOp = cigarElement.getOperator
            cigarOp match {
              case CigarOperator.M | CigarOperator.X | CigarOperator.EQ =>
                var currPosition = 0
                while (currPosition < cigarOpLength) {
                  val subIndex = position % numSubArrays
                  val index = position / numSubArrays

                  if (!covMap.keySet.contains(cr.contigName, index)) {
                    covMap += (cr.contigName, index) -> (Array.ofDim[Int](numSubArrays,coverageHistParam.buckets.length),Array.fill[Int](subArraySize)(0) )
                  }
                  val params = coverageHistParam.buckets.sortBy(r=>r)
                  if(coverageHistParam.histType == CoverageHistType.MAPQ) {
                    breakable {
                      for (i <- 0 until params.length) {
                        if ( i < params.length-1  && cr.mapq >= params(i) && cr.mapq < params(i+1)) {
                          covMap(cr.contigName, index)._1(subIndex)(i) += 1
                          break
                        }
                      }

                    }
                    if (cr.mapq >= params.last) covMap(cr.contigName, index)._1(subIndex)(params.length-1) += 1
                  }
                  else throw new Exception("Unsupported histogram parameter")


                  covMap(cr.contigName, index)._2(subIndex) += 1

                  position += 1
                  currPosition += 1
                  /*add first*/
                  if (outputSize == 0) chrMinMax.append((cr.contigName, position))
                  if (covMap(cr.contigName, index)._2(subIndex) == 1) outputSize += 1

                }
              case CigarOperator.N | CigarOperator.D => position += cigarOpLength
              case _ => None
            }
          }
          val currLength = position - cigarStartPosition
          if (maxCigarLength < currLength) maxCigarLength = currLength
          lastPosition = position
          lastChr = cr.contigName
        }
        chrMinMax.append((lastChr, lastPosition))
        Array(PartitionCoverageHist(covMap, maxCigarLength, outputSize, chrMinMax.toArray)).iterator
      }.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val maxCigarLengthGlobal = partCov.map(r => r.maxCigarLength).reduce((a, b) => scala.math.max(a, b))
    lazy val combOutput = partCov.mapPartitions { partIterator =>
      /*split for reduction basing on position and max cigar length across all partitions - for gap alignments*/
      val partitionCoverageArray = (partIterator.toArray)
      val partitionCoverage = partitionCoverageArray(0)
      val chrMinMax = partitionCoverage.chrMinMax
      lazy val output = new Array[Array[CoverageRecordHist]](2)
      lazy val outputArray = new Array[CoverageRecordHist](partitionCoverage.outputSize)
      lazy val outputArraytoReduce = new ArrayBuffer[CoverageRecordHist]()
      val covMap = partitionCoverage.covMap
      var cnt = 0
      for (key <- covMap.keys) {
        var locCnt = 0
        val covs = covMap.get(key).get
        for (i<-0 until covs._1.length) {
          if (covs._2(i) > 0) {
            val position = key._2 * 10000 + locCnt
            if(key._1 == chrMinMax.head._1 && position <= chrMinMax.head._2 + maxCigarLengthGlobal ||
              key._1 == chrMinMax.last._1 && position >= chrMinMax.last._2 - maxCigarLengthGlobal )
              outputArraytoReduce.append(CoverageRecordHist(sampleId,key._1,position,covs._1(i),covs._2(i)))
            else
              outputArray(cnt) = CoverageRecordHist(sampleId,key._1,position ,covs._1(i),covs._2(i))
            cnt += 1
          }
          locCnt += 1
        }
      } /*only records from the beginning and end of the partition for reduction the rest pass-through */
      output(0) = outputArray.filter(r=> r!=null )
      output(1) = outputArraytoReduce.toArray
      Iterator(output)
    }
    //partCov.unpersist()
    lazy val covReduced =  combOutput.flatMap(r=>r.array(1)).map(r=>((r.chr,r.position),r))
      .reduceByKey((a,b)=>CoverageRecordHist(sampleId,a.chr,a.position,sumArrays(a.coverage,b.coverage),a.coverageTotal+b.coverageTotal)).map(_._2)
    partCov.unpersist()
    combOutput.flatMap(r => (r.array(0)))
      .union(covReduced)
  }
  private def sumArrays(a:Array[Int], b:Array[Int]) ={
    val out = new Array[Int](a.length)
    for(i<- 0 until a.length){
      out(i) = a(i) + b(i)
    }
    out
  }
}

object CoverageReadFunctions {

  implicit def addCoverageReadFunctions(rdd: RDD[BAMRecord]) = {
    new CoverageReadFunctions(rdd)

  }
}

