
import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)
case class PosRecord(contigName:String,start:Int,end:Int)
val alignments = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat]("/data/granges/NA12878.ga2.exome.maq.recal.bam").map(_._2.get).map(r=>PosRecord(r.getContig,r.getStart,r.getEnd))

val reads=alignments.toDF
reads.createOrReplaceTempView("reads")

val targets = spark.read.parquet("/data/granges/tgp_exome_hg18.adam")
targets.createOrReplaceTempView("targets")


val query="""    SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
            |         ON (targets.contigName=reads.contigName
            |         AND
            |         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
            |         AND
            |         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
            |         )
            |         GROUP BY targets.contigName,targets.start,targets.end"""


time(spark.sql(query).show)



import htsjdk.samtools.ValidationStringency

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)
case class PosRecord(contigName:String,start:Int,end:Int)
val alignments = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat]("file:///data/samples//NA12878/NA12878.ga2.exome.maq.recal.bam").map(_._2.get).map(r=>PosRecord(r.getContig,r.getStart,r.getEnd))

val reads=alignments.toDF
reads.createOrReplaceTempView("reads")

val targets = spark.read.parquet("file:///data/samples/NA12878/tgp_exome_hg18.adam")
targets.createOrReplaceTempView("targets")


val query="""    SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
            |         ON (targets.contigName=reads.contigName
            |         AND
            |         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
            |         AND
            |         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
            |         )
            |         GROUP BY targets.contigName,targets.start,targets.end"""




val query2="""SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
ON (targets.contigName=reads.contigName
  AND
  CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
  AND
  CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
)
GROUP BY targets.contigName,targets.start,targets.end
having contigName='chr1' AND    start=20138 AND  end=20294"""


time(spark.sql(query2))





import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim

sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)
case class PosRecord(contigName:String,start:Int,end:Int)
val alignments = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat]("/data/granges/NA12878.ga2.exome.maq.recal.bam").map(_._2.get).map(r=>PosRecord(r.getContig,r.getStart,r.getEnd))

val reads=alignments.toDF
reads.createOrReplaceTempView("reads")

val targets = spark.read.parquet("/data/granges/tgp_exome_hg18.adam")
targets.createOrReplaceTempView("targets")


val query="""    SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
            |         ON (targets.contigName=reads.contigName
            |         AND
            |         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
            |         AND
            |         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
            |         )
            |         GROUP BY targets.contigName,targets.start,targets.end"""




val query2="""SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
ON (targets.contigName=reads.contigName
  AND
  CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
  AND
  CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
)
GROUP BY targets.contigName,targets.start,targets.end
having contigName='chr1' AND    start=20138 AND  end=20294"""

val query3="""SELECT reads.contigName,reads.start,reads.end FROM reads JOIN targets
ON (targets.contigName=reads.contigName
  AND
  CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
  AND
  CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
)
where targets.contigName='chr1' AND  targets.start=20138 AND  targets.end=20294"""

spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
spark.sql(query3).write.csv("/data/granges/bam_chr1_20138_20294.csv")






import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim



val reads = spark.read.parquet("/data/granges/NA12878.ga2.exome.maq.recal.adam")
reads.createOrReplaceTempView("reads")

val targets = spark.read.parquet("/data/granges/tgp_exome_hg18.adam")
targets.createOrReplaceTempView("targets")




val query3="""SELECT reads.contigName,reads.start,reads.end FROM reads JOIN targets
ON (targets.contigName=reads.contigName
  AND
  CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
  AND
  CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
)
where targets.contigName='chr1' AND  targets.start=20138 AND  targets.end=20294"""

spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
spark.sql(query3).write.csv("/data/granges/adam_chr1_20138_20294.csv")