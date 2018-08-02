package org.biodatageeks.preprocessing.coverage

import java.io.File

import htsjdk.samtools.{BAMFileReader, CigarOperator, SamReaderFactory, ValidationStringency}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, BAMBDGInputFormat, BAMInputFormat, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

import scala.collection.mutable
import htsjdk.samtools._
import org.apache.spark.broadcast.Broadcast
//import com.intel.gkl.compression._

import htsjdk.samtools.util.zip.InflaterFactory
import java.util.zip.Inflater


case class CovRecord(contigName:String,start:Int,end:Int, cov:Short)

object CoverageMethodsMos {

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

  def readsToEventsArray(reads:RDD[SAMRecordWritable])   = {
    reads.mapPartitions{
      p =>
        val contigLengthMap = new mutable.HashMap[String, Int]()
        val contigEventsMap = new mutable.HashMap[String, (Array[Short],Int,Int,Int)]()
        val contigStartStopPartMap = new mutable.HashMap[(String),Int]()
        val cigarMap = new mutable.HashMap[String, Int]()
        //var lastContig: String = null
        //var lastPosition = 0
        while(p.hasNext){
          val r = p.next()
          val read = r.get()
          val contig = read.getContig
          if(contig != null && read.getFlags!= 1796) {
            if (!contigLengthMap.contains(contig)) { //FIXME: preallocate basing on header, n
              val contigLength = read.getHeader.getSequence(contig).getSequenceLength
              //println(s"${contig}:${contigLength}:${read.getStart}")
              contigLengthMap += contig -> contigLength
              contigEventsMap += contig -> (new Array[Short](contigLength-read.getStart+10), read.getStart,contigLength-1, contigLength)
              contigStartStopPartMap += s"${contig}_start" -> read.getStart
              cigarMap += contig -> 0
            }
            val cigarIterator = read.getCigar.iterator()
            var position = read.getStart
            //val contigLength = contigLengthMap(contig)
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
            (r._1,(r._2._1.slice(0,maxIndex+1),r._2._2,r._2._2+maxIndex,r._2._4,cigarMap(r._1)) )// add max cigarLength
          }
          )
        output.iterator
    }

  }

  def reduceEventsArray(covEvents: RDD[(String,(Array[Short],Int,Int,Int))]) =  {
    covEvents.reduceByKey((a,b)=> mergeArrays(a,b))
  }

  def eventsToCoverage(sampleId:String,events: RDD[(String,(Array[Short],Int,Int,Int))]) = {
    events
      .mapPartitions{ p => p.map(r=>{
        val contig = r._1
        val covArrayLength = r._2._1.length
        var cov = 0
        var ind = 0
        val posShift = r._2._2

        val result = new Array[CovRecord](covArrayLength)
        var i = 0
        var prevCov = 0
        var blockLength = 0
        while(i < covArrayLength){
          cov += r._2._1(i)
          if(cov >0) {
            if(prevCov>0 && prevCov != cov) {
              result(ind) = CovRecord(contig,i+posShift - blockLength, i + posShift-1, prevCov.toShort)
              blockLength = 0
              ind += 1
            }
            blockLength +=1
            prevCov = cov
          }
          i+= 1
        }
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
