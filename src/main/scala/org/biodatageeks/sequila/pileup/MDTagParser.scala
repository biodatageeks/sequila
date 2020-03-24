package org.biodatageeks.sequila.pileup

import java.io.File

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.samtools.{Cigar, CigarOperator, SAMRecord}
import org.apache.spark.sql.SparkSession
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.seqdoop.hadoop_bam.BAMBDGInputFormat
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._
import scala.util.matching.Regex
import util.control.Breaks._

case class MDOperator(length: Int, base: Char) //S means to skip n positions, not fix needed
case class RefDiff(contig: String,  pos:Int, diff: Array[Char]) //start position and difference - length = 1 SNP, length > 1 Insertion :FIXME use byte array instead os str for performance
case class PartialReffDiff(diff : Array[RefDiff], leftOver: Int)
object MDTagParser extends BDGAlignFileReaderWriter[BAMBDGInputFormat]{

  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)
  val pattern = "([0-9]+)\\^?([A-Za-z]+)?".r


  def getRefDiffFromRead(r: SAMRecord) = {

//    logger.debug(s"Processing read: ${r.getReadName}" )
//    logger.debug(s"MD: ${r.getAttribute("MD")}" )
//    logger.debug(s"CIGAR: ${r.getCigarString}" )
//    logger.debug(s"seq: ${r.getReadString}")
//    logger.debug(s"contig ${r.getContig}")
//    logger.debug(s"start ${r.getStart}")
//    logger.debug(s"stop: ${r.getEnd}")

    val diffs = ArrayBuffer[RefDiff]()
    if(r.getAttribute("MD") == null) {
      logger.warn(s"MD tag for read ${r.getReadName} is missing. Skipping it.")
    }
    else{
      val alignmentBlocks = r.getAlignmentBlocks
//      logger.debug(s"Read has ${alignmentBlocks.size()} aligment blocks to process")
      var ind = 0
      val leftOver =  0
      val mdOperators = parseMDTag(r.getAttribute("MD").toString, pattern)
      if (alignmentBlocks !=  null  && alignmentBlocks.size() > 0){
        for(b <- alignmentBlocks.asScala){
            val deletions = {
              var alignBlockCnt = 0
              var delCnt = 0
              val cigar = r.getCigar.iterator()
              while(cigar.hasNext && alignBlockCnt <= ind){
                val el = cigar.next()
                if(el.getOperator == CigarOperator.MATCH_OR_MISMATCH)  alignBlockCnt += 1
                else if(el.getOperator == CigarOperator.DELETION && alignBlockCnt == ind) delCnt += el.getLength
              }
              delCnt
            }
//            val contig = r.getContig
//            val refStart = b.getReferenceStart - deletions
//            val refEnd = b.getReferenceStart + b.getLength - 1


//              val result  =  getRef(r.getReadString, r.getCigar, r.getAttribute("MD").toString, ind, leftOver, mdOperators)
               val result  =  getRefDiff(r.getReadString, r.getContig, r.getCigar, ind, leftOver, mdOperators, r.getStart + deletions)
              //val reconstructedSeq =  result._1
              if(result.diff != null) diffs ++= result.diff
              //            //          val isEqual
          //            = compareToRefSeq(reconstructedSeq, contig, refStart, refEnd)
              //            //          logger.debug(s"Comparing to ref ${isEqual.toString.toUpperCase}")
              //            //          if (!isEqual) throw new Exception("Bad ref seq reconstruction")
              ind += 1
              //            leftOver = result._2

        }
      }

    }
  diffs.toArray
  }

  private def getRefDiff(seq: String, contig: String, cigar: Cigar, blockNum: Int = 0, leftOver:Int = 0, mdOperators:Array[MDOperator], refStart: Int): PartialReffDiff = {
    val cigarElements = cigar.getCigarElements
    //fast if matches return seq, both MD and cigar is one block
    if(mdOperators.length == 1
      && cigarElements.size() == 1 && cigarElements.get(0).getOperator ==  CigarOperator.MATCH_OR_MISMATCH ) {
      PartialReffDiff(null, -1)
    }

    //fixing SNPs, cigar is one block size
    else if (cigarElements.size() == 1 && cigarElements.get(0).getOperator ==  CigarOperator.MATCH_OR_MISMATCH) {
      val result = applyMDTagDiff(seq, contig, mdOperators, 0, cigarElements.get(0).getLength, 0,leftOver, refStart)
//      logger.debug(s"Seq aft: ${result._1}")
      result
    }
    //handling INDELS, multiple aligment blocks
    else {
//      logger.debug("Processing complex CIGAR with INDELs")
//      logger.debug(s"Processing ${blockNum}(+1) alignment block of ${cigar.toString}")

      var startPos = 0
      val cigarElements = cigar.getCigarElements
      var ind = 0
      var cigarInd = 0
      var insertInd = 0
      while(ind <= blockNum) {
        val cigarElement = cigarElements.get(cigarInd)
        val cigarOp = cigarElement.getOperator
        if (cigarOp == CigarOperator.MATCH_OR_MISMATCH) {
          ind += 1
        }
        if (cigarOp == CigarOperator.MATCH_OR_MISMATCH || cigarOp == CigarOperator.INSERTION || cigarOp == CigarOperator.SOFT_CLIP) {
          startPos += cigarElement.getLength
        }
        if (cigarOp == CigarOperator.INSERTION || cigarOp == CigarOperator.SOFT_CLIP) {
          insertInd += cigarElement.getLength
        }
        cigarInd += 1
      }
      if (ind > 0) {
        cigarInd = cigarInd - 1
        startPos = startPos - cigarElements.get(cigarInd).getLength
      }

      val blockLength =  cigarElements.get(cigarInd).getLength
      val result = applyMDTagDiff(seq, contig, mdOperators, startPos, blockLength, insertInd, leftOver, refStart - insertInd)
//      logger.debug(s"Seq aft: ${result._1}")
      result
    }
  }
  private def getRef(seq : String, cigar: Cigar, mdTag: String, blockNum: Int = 0, leftOver:Int = 0, mdOperators:Array[MDOperator]) = {

    val onlyDigits = "^[0-9]+$"
    val cigarElements = cigar.getCigarElements
    //fast if matches return seq, both MD and cigar is one block
    if(mdTag.matches(onlyDigits)
      && cigarElements.size() == 1 && cigarElements.get(0).getOperator ==  CigarOperator.MATCH_OR_MISMATCH ) {
      logger.debug(s"Seq aft: ${seq}")
      (seq,0)
    }

    //fixing SNPs, cigar is one block size
    else if (cigarElements.size() == 1 && cigarElements.get(0).getOperator ==  CigarOperator.MATCH_OR_MISMATCH) {

      val mdOperators = parseMDTag(mdTag, pattern)
      val result = applyMDTag(seq,mdOperators, 0, cigarElements.get(0).getLength, 0,leftOver)
      logger.debug(s"Seq aft: ${result._1}")
      result
    }
    //handling INDELS, multiple aligment blocks
    else {
      logger.debug("Processing complex CIGAR with INDELs")
      logger.debug(s"Processing ${blockNum}(+1) alignment block of ${cigar.toString}")

      var startPos = 0
      val cigarElements = cigar.getCigarElements
      var ind = 0
      var cigarInd = 0
      var insertInd = 0
      while(ind <= blockNum) {
        val cigarElement = cigarElements.get(cigarInd)
        val cigarOp = cigarElement.getOperator
        if (cigarOp == CigarOperator.MATCH_OR_MISMATCH) {
          ind += 1
        }
        if (cigarOp == CigarOperator.MATCH_OR_MISMATCH || cigarOp == CigarOperator.INSERTION || cigarOp == CigarOperator.SOFT_CLIP) {
          startPos += cigarElement.getLength
        }
        if (cigarOp == CigarOperator.INSERTION || cigarOp == CigarOperator.SOFT_CLIP) {
          insertInd += cigarElement.getLength
        }
        cigarInd += 1
      }
      if (ind > 0) {
        cigarInd = cigarInd - 1
        startPos = startPos - cigarElements.get(cigarInd).getLength
      }

      val blockLength =  cigarElements.get(cigarInd).getLength
      val result = applyMDTag(seq, mdOperators, startPos, blockLength, insertInd, leftOver)
      logger.debug(s"Seq aft: ${result._1}")
      result
    }
  }

  def parseMDTag(t : String, pattern: Regex) = {
//    logger.debug(s"Parsing MD tag: ${t}")

    if (isAllDigits(t)) {
      Array[MDOperator](MDOperator(t.toInt, 'S'))
    }
    else {
      val ab = new ArrayBuffer[MDOperator]()
      val matches = pattern
        .findAllIn(t)
      while (matches.hasNext) {
        val m = matches.next().toUpperCase
//        logger.debug(s"MD operator: ${m}")
        if(m.last.isLetter && !m.contains('^') ){
          val skipPos = m.dropRight(1).toInt
          ab.append(MDOperator(skipPos, 'S') )
          ab.append(MDOperator(0, m.last.toUpper))
        }
        else if (m.last.isLetter && m.contains('^') ){ //encoding deletions as lowercase
          val arr =  m.split('^')
          val skipPos = arr.head.toInt
          ab.append(MDOperator(skipPos, 'S') )
          arr(1).foreach { b =>
            ab.append(MDOperator(0, b.toLower))
          }
        }
        else ab.append(MDOperator(m.toInt, 'S') )
      }
      ab.toArray
    }

  }

  private def applyMDTag(s: String,t: Array[MDOperator], pShift: Int = 0, blockLength: Int, inserts: Int = 0, lo:  Int) = {
    logger.debug(s"Starting applying MD op at pos: ${pShift} with block length: ${blockLength}")
    val seqFixed = StringBuilder.newBuilder
    var ind = 0
    var  remaingBlockLength = blockLength
    var  leftOver = lo
    var isFirstOpInBlock = true
    val tSize = t.length
    var i = 0
    while (i < tSize) {
      val op = t(i)
      logger.debug(s"Operator: ${op.base}, length: ${op.length}")
      if(ind < blockLength + pShift) {
        if(op.base == 'S' ) {
          logger.debug(s"Index: ${ind}, inserts:  ${inserts}, blockLen:  ${remaingBlockLength}")
          val startPos =   { if(isFirstOpInBlock) pShift else 0 } + ind
          val shift  = math.min(remaingBlockLength,  {if(isFirstOpInBlock)  op.length - pShift + inserts
          else if (op.length > remaingBlockLength) remaingBlockLength  else op.length })
          logger.debug(s"shift:  ${shift}")
          val endPos =  startPos + shift
          if ((endPos > pShift || ind + op.length >  pShift) && shift > 0  && remaingBlockLength > 0){
            logger.debug(s"Applying MD op: ${op.toString}, ${ind}")
            val startPosTrim = if(startPos < pShift) pShift else startPos
            logger.debug(s"LeftOver from the previous block: ${leftOver}")
            val correctedShift = {if(leftOver > 0  && leftOver < shift) leftOver else shift }
            val endPosTrim =  startPosTrim + correctedShift
            logger.debug(s"start: ${startPosTrim}, end: ${endPosTrim}")
            val seqToAppend = s.substring(  startPosTrim ,  endPosTrim )
            seqFixed.append(seqToAppend)
            logger.debug(s"Append seq length: ${seqToAppend.length} by skipping with ${seqToAppend}, start: ${startPosTrim}, end: ${endPosTrim}")
            if((isFirstOpInBlock && op.length > remaingBlockLength )|| (leftOver > remaingBlockLength) )
              logger.debug(s"Truncating operator from ${if(leftOver >0) leftOver else op.length }S to ${remaingBlockLength}S, " +
                s"leftover to next block ${if(leftOver >0)  leftOver-remaingBlockLength else op.length-remaingBlockLength}S")
            if (correctedShift == leftOver )
              leftOver = 0
            else if (leftOver > 0 &&  leftOver >= remaingBlockLength )
              leftOver -= remaingBlockLength
            else
              leftOver = op.length - remaingBlockLength
            remaingBlockLength -= seqToAppend.length
            ind = endPosTrim
          }
          else if  ( ind + op.length >=  pShift  && op.length > remaingBlockLength ) ind += op.length
          else if  ( shift > 0  && op.length > remaingBlockLength ) ind += op.length
          else ind = endPos

          isFirstOpInBlock = false
        }
        else if(op.base != 'S' ){ //current block
          if(ind >= pShift && remaingBlockLength > 0){
            seqFixed.append(op.base.toUpper.toString)
            logger.debug(s"Append seq length: 1, at pos ${ind} with base ${op.base.toString}")
            if (op.base.isUpper) remaingBlockLength -= 1
          }
          if (op.base.isUpper) {
            ind += 1

          }
        }
      }
      else {
        ind += op.length
        logger.debug(s"Skipping MD op: ${op.toString}, ${ind}")
      }
      i += 1
    }
    (seqFixed.toString(), leftOver)
  }


  private def applyMDTagDiff( s: String, contig:String, t: Array[MDOperator], pShift: Int = 0, blockLength: Int, inserts: Int = 0, lo:  Int, refStart: Int):PartialReffDiff = {
//    logger.debug(s"Starting applying MD op at pos: ${pShift} with block length: ${blockLength}")
    val diff = new ArrayBuffer[RefDiff]()
    var ind = pShift
    var  remaingBlockLength = blockLength
    var  leftOver = lo
    var isFirstOpInBlock = true
    val tSize = t.length
    var i = 0
    while (i < tSize) {
      val op = t(i)
//      logger.debug(s"Operator: ${op.base}, length: ${op.length}")
      if(ind < blockLength + pShift) {
        if(op.base == 'S' ) {
//          logger.debug(s"Index: ${ind}, inserts:  ${inserts}, blockLen:  ${remaingBlockLength}")
          val startPos =   { if(isFirstOpInBlock) pShift else 0 } + ind
          val shift  = math.min(remaingBlockLength,  {if(isFirstOpInBlock)  op.length - pShift + inserts
          else if (op.length > remaingBlockLength) remaingBlockLength  else op.length })
//          logger.debug(s"shift:  ${shift}")
          val endPos =  startPos + shift
          if ((endPos > pShift || ind + op.length >  pShift) && shift > 0  && remaingBlockLength > 0){
//            logger.debug(s"Applying MD op: ${op.toString}, ${ind}")
            val startPosTrim = if(startPos < pShift) pShift else startPos
//            logger.debug(s"LeftOver from the previous block: ${leftOver}")
            val correctedShift = {if(leftOver > 0  && leftOver < shift) leftOver else shift }
            val endPosTrim =  startPosTrim + correctedShift
//            logger.debug(s"start: ${startPosTrim}, end: ${endPosTrim}")
            if((isFirstOpInBlock && op.length > remaingBlockLength )|| (leftOver > remaingBlockLength) )
//              logger.debug(s"Truncating operator from ${if(leftOver >0) leftOver else op.length }S to ${remaingBlockLength}S, " +
//                s"leftover to next block ${if(leftOver >0)  leftOver-remaingBlockLength else op.length-remaingBlockLength}S")
            if (correctedShift == leftOver )
              leftOver = 0
            else if (leftOver > 0 &&  leftOver >= remaingBlockLength )
              leftOver -= remaingBlockLength
            else
              leftOver = op.length - remaingBlockLength
            remaingBlockLength -=  endPosTrim- startPosTrim
            ind = endPosTrim
          }
          else if  ( ind + op.length >=  pShift  && op.length > remaingBlockLength ) ind += op.length
          else if  ( shift > 0  && op.length > remaingBlockLength ) ind += op.length
          else ind = endPos

          isFirstOpInBlock = false
        }
        else if(op.base != 'S' ){ //current block
          if(ind >= pShift && remaingBlockLength > 0){
            if(op.base.isUpper){
              val d = RefDiff(contig, refStart + ind, Array(s.substring(ind, ind + 1)(0)) )
//              logger.debug(s"Appending diff contig: ${d.contig}, pos: ${d.pos}, base: ${d.diff.mkString("|")}")
              diff.append(d)
              remaingBlockLength -= 1
            }
          }
          if (op.base.isUpper) {
            ind += 1

          }
        }
      }
      else {
        ind += op.length
//        logger.debug(s"Skipping MD op: ${op.toString}, ${ind}")
      }
      i += 1
    }
    PartialReffDiff(diff.toArray, leftOver)
  }


  private def compareToRefSeq(fasta: IndexedFastaSequenceFile, seq:String, contig: String, start : Int, end : Int ) = {

    val refSeq = fasta.getSubsequenceAt( contig, start.toLong, end.toLong)
    logger.debug(s"Seq ref: ${refSeq.getBaseString.toUpperCase}")
    seq.equalsIgnoreCase(refSeq.getBaseString)

  }

  private def isAllDigits(s: String) : Boolean = {
    var numDigits = 0
    val len = s.length
    var i = 0
      while(i < len){
        if(s(i).isDigit) numDigits += 1
        else return false
        i += 1
      }
    if(numDigits == len) true else false
  }
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[4]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("INFO")
    val sqlSession = sparkSession.sqlContext
//    val bamRecords = readBAMFile(sqlSession,"/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.md.bam")
    val bamRecords = readBAMFile(sqlSession,"/Users/marek/data/NA12878.proper.wes.md.bam")
//    val bamRecords = readBAMFile(sqlSession,"/Users/marek/git/forks/bdg-sequila/src/test/resources/NA12878.slice.md.bam")

//      val fasta = new IndexedFastaSequenceFile(new File("/Users/marek/data/hs37d5.fa"))
    val fasta = new IndexedFastaSequenceFile(new File("/Users/marek/data/Homo_sapiens_assembly18.fasta"))



//    val ss = SequilaSession(sparkSession)
//    SequilaRegister.register(ss)
//    ss.sql("DROP TABLE IF EXISTS reads_exome")
//    ss.sql("""CREATE TABLE IF NOT EXISTS reads_exome USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource OPTIONS(path '/Users/marek/data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.md.bam')""")
//    sparkSession.time {
//      ss.sql(s"SELECT * FROM bdg_coverage('reads_exome','NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.md', 'blocks')").count()
//    }

    sparkSession.time{
      val records = bamRecords
//          .filter(r =>
//               r.getReadName=="SRR622461.74266492"
//            || r.getReadName=="SRR622461.74266917"
//            || r.getReadName=="SRR622461.74268065"
//            || r.getReadName=="SRR622461.74274597"
//             ||  r.getReadName=="SRR622461.74274711"
//             || r.getReadName=="SRR622461.74266195"
//            || r.getReadName=="SRR622461.74268406"
//            || r.getReadName =="SRR622461.75291121"
//            || r.getReadName=="SRR622461.74268422"
//            || r.getReadName=="SRR622461.74274611"
//            || r.getReadName=="SRR622461.74276804"
//            ||  r.getReadName=="SRR622461.74279756"
//            || r.getReadName=="SRR622461.74271055"
//            || r.getReadName=="SRR622461.74323993"
//             ||  r.getReadName=="SRR622461.74274709"
//          )

        .map(getRefDiffFromRead(_))
          .count()

//        println(records.first().mkString("|"))
      logger.info(s"Total records processed: ${records}")
    }




  }


}
