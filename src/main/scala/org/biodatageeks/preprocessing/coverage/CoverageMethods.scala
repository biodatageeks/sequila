package org.biodatageeks.preprocessing.coverage



import htsjdk.samtools.{BAMFileReader, CigarOperator, SamReaderFactory, ValidationStringency}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import htsjdk.samtools._
import org.apache.spark.broadcast.Broadcast
import org.apache.log4j.Logger


abstract class AbstractCovRecord {
  def key = (this.contigName+this.start.toString+this.end.toString).hashCode
  def contigName: String
  def start: Int
  def end: Int
  //def cov: T
}


case class CovRecord(contigName:String, start:Int,end:Int, cov:Short)
  extends AbstractCovRecord


//for various coverage windows operations
case class CovRecordWindow(contigName:String, start:Int,end:Int, cov:Float, overLap:Option[Int] = None)
extends AbstractCovRecord

object CoverageMethodsMos {
  val logger = Logger.getLogger(this.getClass.getCanonicalName)


  def mergeArrays(a:(Array[Short],Int,Int,Int),b:(Array[Short],Int,Int,Int)) = {
    val c = new Array[Short](math.min(math.abs(a._2 - b._2) + math.max(a._1.length, b._1.length), a._4))
    val lowerBound = math.min(a._2, b._2)

    var i = lowerBound
    while (i <= c.length + lowerBound - 1) {
      c(i - lowerBound) = ((if (i >= a._2 && i < a._2 + a._1.length) a._1(i - a._2) else 0) + (if (i >= b._2 && i < b._2 + b._1.length) b._1(i - b._2) else 0)).toShort
      i += 1
    }
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
              val cigarOp = cigarElement.getOperator
              // add to cigarlen depending on operator
              if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ ||cigarOp == CigarOperator.N || cigarOp == CigarOperator.D)
                currCigarLength += cigarOpLength
              if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
                eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,true) //Fixme: use variable insteaad of lookup to a map
                position += cigarOpLength
                eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,false)
              }
              else  if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D) position += cigarOpLength
              //else  position += cigarOpLength
            }
            if(currCigarLength > cigarMap(contig)) {
//              logger.warn(s"#### change of maxcigar ${cigarMap(contig)} -> $currCigarLength for read start:${read.getStart}, end: ${read.getEnd} ")
              cigarMap(contig) = currCigarLength

            }

          }
        }

        lazy val output = contigEventsMap // cut-off last zeroes
          .map(r=>
          {
            var maxIndex = 0
            var i = 0
            while(i < r._2._1.length ){

              if(r._2._1(i) != 0) maxIndex = i
              i +=1
            }
            (r._1,(r._2._1.slice(0,maxIndex+1),r._2._2, r._2._2+maxIndex ,r._2._4, cigarMap(r._1)) )// add max cigarLength // add first read index
            //contig, cov[], start, maxpos, contiglen , maxcigar
          }
          )
        output.iterator
    }
  }



  def reduceEventsArray(covEvents: RDD[(String,(Array[Short],Int,Int,Int))]) =  {
    covEvents.reduceByKey((a,b)=> mergeArrays(a,b))
  }


  @inline def addFirstBlock (contig:String, contigMin: Int, posShift: Int, blocksResult:Boolean, allPos:Boolean, ind: Int,result: Array[AbstractCovRecord]) = {
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

  @inline def addLastBlock(contig: String, contigMax: Int, contigLength: Int, maxPosition: Int, blocksResult: Boolean,  allPos:Boolean, ind: Int, result: Array[AbstractCovRecord]) = {
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


  @inline def addLastWindow(contig: String, windowLength: Option[Int], posShift: Int, i: Int, covSum: Int, cov: Int, ind: Int, result: Array[AbstractCovRecord]) = {
    var indexShift = ind
    var sum = covSum
    if (i%windowLength.get != 0) { // add last window
      val winLen = windowLength.get
      val windowStart =  ( (i+posShift) / winLen)  * windowLength.get
      val windowEnd = windowStart + winLen - 1
      //          val lastWindowLength = (i + posShift) % winLen  // (i + posShift -1) % winLen // -1 current
      val lastWindowLength = (i + posShift) % winLen - 1 // HACK to fix last window (omit last element)
      sum -= cov   // HACK to fix last window (substract last element)

      result(ind) = CovRecordWindow(contig, windowStart, windowEnd, sum/lastWindowLength.toFloat, Some (lastWindowLength))
      indexShift+=1
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
        var covSum = 0

        val firstBlockMaxLength = posShift-1
        val lastBlockMaxLength = r._2._4 - maxPosition //TODO: double check meaning of maxCigar

        // preallocate maximum size of result, assuming first and last blocks are added in perbase manner
        //IDEA: consider counting size within if-else statements

        val result = new Array[AbstractCovRecord](firstBlockMaxLength + covArrayLength + lastBlockMaxLength)

        logger.debug (s"$contig shift $posShift")
        logger.debug (s"size: ${firstBlockMaxLength + covArrayLength + lastBlockMaxLength}")

        var i = 0
        var prevCov = 0
        var blockLength = 0


        if (windowLength.isEmpty) { // BLOCKS & BASES (NON-WINDOW) COVERAGE CALCULATIONS
          ind = addFirstBlock(contig, contigMinMap(contig)._1, posShift, blocksResult, allPos, ind, result)  // add first block if necessary (if current positionshift is equal to the earliest read in the contig)

          while (i < covArrayLength) {
            cov += r._2._1(i)

            if (!blocksResult) {                  // per-base output
              if (i != covArrayLength - 1 ) { //HACK. otherwise we get doubled CovRecords for partition boundary index
                if (allPos ||  cov != 0) { // show all positions or only non-zero
                  result(ind) = CovRecord(contig, i + posShift, i + posShift, cov.toShort)
                  ind += 1
                }
              }
            } else {                              // blocks output
              if (prevCov >= 0 && prevCov != cov && i > 0) { // for the first element we do not write block
                if (allPos ||  prevCov != 0) { // show all positions or only non-zero
                  result(ind) = CovRecord(contig, i + posShift - blockLength, i + posShift - 1, prevCov.toShort)
                  blockLength = 0
                  ind += 1
                }
              }
              blockLength += 1
              prevCov = cov
            }
            i += 1
          }

          ind = addLastBlock (contig, contigMinMap(contig)._2, r._2._4, maxPosition, blocksResult, allPos, ind, result)

          result.take(ind).iterator

        } else {          // FIXED - WINDOW COVERAGE CALCULATIONS
          while (i < covArrayLength) {
            cov += r._2._1(i)

            if ((i + posShift) % windowLength.get == 0 && (i + posShift) > 0) {
                val length =
                  if (i < windowLength.get) i
                  else windowLength.get

                val winLen = windowLength.get
                val windowStart = (((i + posShift) / winLen) - 1) * winLen
                val windowEnd = windowStart + winLen - 1
                result(ind) = CovRecordWindow(contig, windowStart, windowEnd, covSum / length.toFloat, Some(length))
                covSum = 0
                ind += 1
              }

            covSum += cov
            i += 1

          }
          ind = addLastWindow(contig, windowLength, posShift, i, covSum, cov, ind, result)

          result.take(ind).iterator
        }

      })
      }.flatMap(r=>r)
  }


  def upateContigRange(b:Broadcast[UpdateStruct],covEvents: RDD[(String,(Array[Short],Int,Int,Int,Int))]) = {
    logger.info(s"### covEvents count ${covEvents.count()}")

   val newCovEvents = covEvents.map {
     c => {
       logger.debug (s"updating partition ${c._1}, ${c._2._2}")
       val upd = b.value.upd
       val shrink = b.value.shrink
       val(contig,(eventsArray,minPos,maxPos,contigLength,maxCigarLength)) = c // to REFACTOR
       var eventsArrMutable = eventsArray
       logger.debug(s"#### Update Partition: $contig, min=$minPos max=$maxPos len:${eventsArray.length} span: ${maxPos-minPos} ")

       val updArray = upd.get((contig, minPos)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
         case Some((arr, covSum)) => { // array of covs and cumSum
           arr match {
             case Some(overlapArray) => {
               if (overlapArray.length > eventsArray.length) {
                 eventsArrMutable = eventsArray ++ Array.fill[Short](overlapArray.length - eventsArray.length)(0) // extend array
                 // logger.warn(s"Overlap longer than events arr. Updating array in ev_len=${eventsArrMutable.length}, ov_len=${overlapArray.length}")
               }

               var i = 0
               logger.debug(s"$contig, min=$minPos max=$maxPos updating: ${eventsArrMutable.take(10).mkString(",")} with ${overlapArray.take(10).mkString(",")} and $covSum ")
               eventsArrMutable(i) = (eventsArrMutable(i) + covSum).toShort // add cumSum to zeroth element

               while (i < overlapArray.length) {
                 try {
                   eventsArrMutable(i) = (eventsArrMutable(i) + overlapArray(i)).toShort
                   i += 1
                 }
                 catch {
                   case e: ArrayIndexOutOfBoundsException => logger.error(s" Overlap array length: ${overlapArray.length}, events array length: ${eventsArray.length}")
                   throw e
                 }
               }
               logger.debug(s"$contig, min=$minPos max=$maxPos Updated array ${eventsArrMutable.take(10).mkString(",")}")
               eventsArrMutable
             }
             case None => {
               eventsArrMutable(0) = (eventsArrMutable(0) + covSum).toShort
               eventsArrMutable
             }
           }
         }
         case None => {
           eventsArrMutable
         }
       }
       val shrinkArray = shrink.get( (contig, minPos) ) match {
         case Some(len) => {
            updArray.take(len)
         }
         case None => updArray
       }
       logger.debug(s"#### End of Update Partition: $contig, min=$minPos max=$maxPos len:${eventsArray.length} span: ${maxPos-minPos}")
       (contig, (shrinkArray, minPos, maxPos, contigLength) )
     }
   }
    newCovEvents
  }
}
