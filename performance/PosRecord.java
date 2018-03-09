import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
 sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)
case class PosRecord(chr:String,start:Int,end:Int) 
val alignments = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat]("/data/granges/NA12878.ga2.exome.maq.recal.bam")
.map(_._2.get).map(r=>PosRecord(r.getContig,r.getStart,r.getEnd))

val df=alignments.toDF
df.