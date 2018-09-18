package org.biodatageeks.preprocessing.coverage



import htsjdk.samtools.{BAMFileReader, CigarOperator, SamReaderFactory, ValidationStringency}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import htsjdk.samtools._
import org.apache.spark.broadcast.Broadcast
import org.apache.log4j.Logger


abstract class AbstractCovRecord {
  val key = this.hashCode() ///FIXME: key should only hash of contigName,start,end
  def contigName: String
  def start: Int
  def end: Int
  def cov: Short


  override def equals(obj:Any) = {
    if(obj.isInstanceOf[AbstractCovRecord]) {
     val cov = obj.asInstanceOf[AbstractCovRecord]
      if(cov.contigName == this.contigName && cov.start == this.start && cov.end == this.end) true else false
    }
    else false
  }

}


case class CovRecord(val contigName:String, val start:Int,val end:Int, val cov:Short)
  extends AbstractCovRecord
    with Ordered[CovRecord] {

  override def compare(that: CovRecord): Int = this.start compare that.start
}

//for various coverage windows operations
case class CovRecordWindow(val contigName:String, val start:Int,val end:Int, val cov:Short, val overLap:Option[Int] = None)
extends AbstractCovRecord

object CoverageMethodsMos {
  val logger = Logger.getLogger(this.getClass.getCanonicalName)


  def mergeArrays(a:(Array[Short],Int,Int,Int),b:(Array[Short],Int,Int,Int)) = {
    //val c = new Array[Short](a._4)
    val c = new Array[Short](math.min(math.abs(a._2 - b._2) + math.max(a._1.length, b._1.length), a._4))
    val lowerBound = math.min(a._2, b._2)

    var i = lowerBound
    while (i <= c.length + lowerBound - 1) {
      c(i - lowerBound) = ((if (i >= a._2 && i < a._2 + a._1.length) a._1(i - a._2) else 0) + (if (i >= b._2 && i < b._2 + b._1.length) b._1(i - b._2) else 0)).toShort
      i += 1
    }
    //    for(i<- lowerBound to c.length + lowerBound - 1 ){
    //      c(i-lowerBound) = (( if(i >= a._2 && i < a._2+a._1.length) a._1(i-a._2) else 0)  + (if(i >= b._2 && i < b._2+b._1.length) b._1(i-b._2) else 0)).toShort
    //    }

    (c, lowerBound, lowerBound + c.length, a._4)
  }

  @inline def eventOp(pos:Int, startPart:Int, contig:String, contigEventsMap:mutable.HashMap[String,(Array[Short],Int,Int,Int)], incr:Boolean) ={

    val position = pos - startPart
    if(incr)
      contigEventsMap(contig)._1(position) = (contigEventsMap(contig)._1(position) + 1).toShort
    else
      contigEventsMap(contig)._1(position) = (contigEventsMap(contig)._1(position) - 1).toShort
  }


  def readsToEventsArray(reads:RDD[SAMRecord], filterFlag:Int)   = {
    reads.mapPartitions{
      p =>
        val contigLengthMap = new mutable.HashMap[String, Int]()
        val contigEventsMap = new mutable.HashMap[String, (Array[Short],Int,Int,Int)]()
        val contigStartStopPartMap = new mutable.HashMap[(String),Int]()
        val cigarMap = new mutable.HashMap[String, Int]()
        while(p.hasNext){
          val r = p.next()
          val read = r
          val contig = read.getContig

          // default value of filterFlag 1796:
          // * read unmapped (0x4)
          // * not primary alignment (0x100)
          // * read fails platform/vendor quality checks (0x200)
          // * read is PCR or optical duplicate (0x400)

          // filter out reads with flags that have any of the bits from FILTERFLAG  set
          if(contig != null && (read.getFlags & filterFlag) == 0) {
            if (!contigLengthMap.contains(contig)) { //FIXME: preallocate basing on header, n
              val contigLength = read.getHeader.getSequence(contig).getSequenceLength
              contigLengthMap += contig -> contigLength
              contigEventsMap += contig -> (new Array[Short](contigLength-read.getStart+10), read.getStart,contigLength-1, contigLength)
              //contigEventsMap += contig -> (new Array[Short](contigLength+10), read.getStart,contigLength-1, contigLength)
              contigStartStopPartMap += s"${contig}_start" -> read.getStart
              cigarMap += contig -> 0
            }

            val cigarIterator = read.getCigar.iterator()
            var position = read.getStart

            var currCigarLength = 0
            while(cigarIterator.hasNext){
              val cigarElement = cigarIterator.next()
              val cigarOpLength = cigarElement.getLength
              currCigarLength += cigarOpLength
              val cigarOp = cigarElement.getOperator
              if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
                eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,true) //Fixme: use variable insteaad of lookup to a map
                position += cigarOpLength
                eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,false)
              }
              else  if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D) position += cigarOpLength
            }
            if(currCigarLength > cigarMap(contig)) cigarMap(contig) = currCigarLength

          }
        }
        lazy val output = contigEventsMap
          .map(r=>
          {
            var maxIndex = 0
            var i = 0
            while(i < r._2._1.length ){

              if(r._2._1(i) != 0) maxIndex = i
              i +=1
            }
            (r._1,(r._2._1.slice(0,maxIndex+1),r._2._2,r._2._2+maxIndex,r._2._4,cigarMap(r._1)) )// add max cigarLength // add first read index
          }
          )
        output.iterator
    }
  }



  def reduceEventsArray(covEvents: RDD[(String,(Array[Short],Int,Int,Int))]) =  {
    covEvents.reduceByKey((a,b)=> mergeArrays(a,b))
  }


  @inline def addFirstBlock (contig:String, contigMin: Int, posShift: Int, blocksResult:Boolean, allPos:Boolean, ind: Int,result: Array[CovRecord]) = {
    var indexShift = ind

    if (allPos && posShift == contigMin) {
      logger.info(s"Adding first block for index: ${indexShift}, start: 1 end: ${posShift - 1}, cov: 0")
      if (blocksResult) {
        result(indexShift) = CovRecord(contig, 1, posShift - 1, 0) // add first block, from 1 to posShift-1
        indexShift += 1
      } else {
        for (cnt <- 1 to posShift-1) {
          result(indexShift) = CovRecord(contig, cnt, cnt, 0) // add first block, from 1 to posShift-1
          indexShift += 1
        }
      }
    }
    indexShift
  }

  @inline def addLastBlock(contig: String, contigMax: Int, contigLength: Int, maxPosition: Int, blocksResult: Boolean,  allPos:Boolean, ind: Int, result: Array[CovRecord]) = {
    var indexShift = ind
    if (allPos && maxPosition == contigMax) {
      logger.debug(s"Adding last block for index: ${indexShift}, start: ${maxPosition} end: ${contigLength}, cov: 0")
      if (blocksResult) {
        result(indexShift) = CovRecord(contig, maxPosition, contigLength, 0)
        indexShift += 1
      } else {
        for (cnt <- maxPosition  to contigLength) {
          result(indexShift) = CovRecord(contig, cnt, cnt, 0) // add first block, from 1 to posShift-1
          indexShift += 1
        }
      }
    }
    indexShift
  }


  def eventsToCoverage(sampleId:String, events: RDD[(String,(Array[Short],Int,Int,Int))],
                       contigMinMap: mutable.HashMap [String,(Int,Int)],
                       blocksResult:Boolean, allPos: Boolean, windowLength: Option[Int], targetsTable:Option[String]) : RDD[AbstractCovRecord] = {
    events
      .mapPartitions{ p => p.map(r=>{
        val contig = r._1
        val covArrayLength = r._2._1.length
        var cov = 0
        var ind = 0
        val posShift = r._2._2
        val maxPosition = r._2._3


        val firstBlockMaxLength = posShift-1
        val lastBlockMaxLength = r._2._4 - maxPosition //TODO: double check meaning of maxCigar

        // preallocate maximum size of result, assuming first and last blocks are added in perbase manner
        //IDEA: consider counting size within if-else statements

        val result = new Array[CovRecord](firstBlockMaxLength + covArrayLength + lastBlockMaxLength)

        logger.info (s"size: ${firstBlockMaxLength + covArrayLength + lastBlockMaxLength}")

        var i = 0
        var prevCov = 0
        var blockLength = 0

        // add first block if necessary (if current positionshift is equal to the earliest read in the contig)
        ind = addFirstBlock(contig, contigMinMap(contig)._1, posShift, blocksResult, allPos, ind, result)


        while(i < covArrayLength){
          cov += r._2._1(i)

          if (!blocksResult && windowLength == None) {
            if (i!= covArrayLength - 1) { //HACK. otherwise we get doubled CovRecords for partition boundary index
              result(ind) = CovRecord(contig, i + posShift, i + posShift, cov.toShort)
              ind += 1
            }
          }
          else if(windowLength == None) {
            if (prevCov >= 0 && prevCov != cov && i > 0) { // for the first element we do not write block
              result(ind) = CovRecord(contig, i + posShift - blockLength, i + posShift - 1, prevCov.toShort)
              blockLength = 0
              ind += 1
            }
            blockLength += 1
            prevCov = cov
          }
          i+= 1
        }

        ind = addLastBlock (contig, contigMinMap(contig)._2, r._2._4, maxPosition, blocksResult, allPos, ind, result)

        result.take(ind).iterator
      })
      }.flatMap(r=>r)
  }

  //contigName,covArray,minPos,maxPos,contigLenth,maxCigarLength
  def upateContigRange(b:Broadcast[UpdateStruct],covEvents: RDD[(String,(Array[Short],Int,Int,Int,Int))]) = {
   covEvents.map{
     c => {
       val upd = b.value.upd
       val shrink = b.value.shrink

       val updArray = upd.get( (c._1,c._2._2) ) match {
         case Some(a) => {
           a._1 match {
             case Some(b) => {
               var i = 0
               while (i < b.length) {
                 c._2._1(i) = (c._2._1(i) + b(i)).toShort
                 if (i == 0) c._2._1(0) = (c._2._1(0) + a._2).toShort
                 i += 1
               }
               c._2._1
             }
             case None => {
               c._2._1(0) = (c._2._1(0) + a._2).toShort
               c._2._1
             }
           }
         }
           case None =>{
             c._2._1
           }
       }
       val shrinkArray = shrink.get( (c._1, c._2._2) ) match {
         case Some(len) => {
            updArray.take(len)
         }
         case None => updArray
       }
       (c._1, (shrinkArray,c._2._2,c._2._3,c._2._4) )
     }
   }
  }
}
