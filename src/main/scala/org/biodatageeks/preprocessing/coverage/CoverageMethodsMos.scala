package org.biodatageeks.preprocessing.coverage

import java.io.File

import htsjdk.samtools.{BAMFileReader, CigarOperator, SamReaderFactory, ValidationStringency}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, BAMInputFormat, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

import scala.collection.mutable
import htsjdk.samtools._
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
            }
            val cigarIterator = read.getCigar.iterator()
            var position = read.getStart
            //val contigLength = contigLengthMap(contig)
            while(cigarIterator.hasNext){
              val cigarElement = cigarIterator.next()
              val cigarOpLength = cigarElement.getLength
              val cigarOp = cigarElement.getOperator
              if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
                eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,true) //Fixme: use variable insteaad of lookup to a map
                position += cigarOpLength
                eventOp(position,contigStartStopPartMap(s"${contig}_start"),contig,contigEventsMap,false)
              }
              else  if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D) position += cigarOpLength
            }

          }
        }
        contigEventsMap
          .mapValues(r=>
          {
            var maxIndex = 0
            var i = 0
            while(i < r._1.length ){
              if(r._1(i) != 0) maxIndex = i
              i +=1
            }
            (r._1.slice(0,maxIndex+1),r._2,r._2+maxIndex,r._4)
          }
          ).iterator
    }.reduceByKey((a,b)=> mergeArrays(a,b))

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


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
     .master("local[4]")
     .config("spark.driver.memory", "8g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark
      .sparkContext
      .hadoopConfiguration
      .set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.LENIENT.toString)

    spark
      .sparkContext
      .hadoopConfiguration
      .setInt("mapred.min.split.size", (134217728).toInt)
    spark
      .sparkContext
      .hadoopConfiguration
      //.setBoolean("hadoopbam.bam.use-intel-inflater",true)

    //spark.sparkContext.setLogLevel("INFO")
    lazy val alignments = spark.sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat]("/Users/marek//Downloads/data/NA12878.ga2.exome.maq.recal.bam")
   // .newAPIHadoopFile[LongWritable, SAMRecordWritable,BAMInputFormat]("/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam")

    lazy val events = readsToEventsArray(alignments.map(r => r._2))
    spark.time {
      println(alignments.count)
    }
    spark.time {

      //println(events.count)
    }

    //lazy val combinedRes = combineEvents(events)
    lazy val coverage = eventsToCoverage("test", events)
    spark.time {
     //println(coverage.count())

    }

    val size = 100000000
    val a = new Array[Short](size)

//    spark.time{
//      println ("Running builtin Java's inflate")
//      val f = new File("/Users/marek//Downloads/data/NA12878.ga2.exome.maq.recal.bam")
//      lazy val a =  SamReaderFactory
//        .makeDefault()
//        .disable(SamReaderFactory.Option.EAGERLY_DECODE)
//        .validationStringency(ValidationStringency.LENIENT)
//        .setUseAsyncIo(false)
//      lazy val b = a.open(f)
//      lazy val c = b.iterator()
//      var cnt = 0
//      while(c.hasNext){
//        val x = c.next().getStart
//        cnt += 1
//      }
//      println(cnt)
//
//    }
//   spark.time{
//     println ("Running GKL inflate")
//     val intelDeflaterFactory =  new IntelInflaterFactory()
//
//     val f = new File("/Users/marek//Downloads/data/NA12878.ga2.exome.maq.recal.bam")
//     lazy val a =  SamReaderFactory
//       .makeDefault()
//       .disable(SamReaderFactory.Option.EAGERLY_DECODE)
//       .validationStringency(ValidationStringency.LENIENT)
//       .setUseAsyncIo(false)
//       .inflaterFactory(intelDeflaterFactory)
//     lazy val b = a.open(f)
//     lazy val c = b.iterator()
//     var cnt = 0
//     while(c.hasNext){
//       val x = c.next().getStart
//       cnt += 1
//     }
//     println(cnt)
//   }



  }


}
