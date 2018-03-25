package pl.edu.pw.ii.biodatageeks.tests

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.LongWritable
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.seqdoop.hadoop_bam.{BAMInputFormat, SAMRecordWritable}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

case class Region(contigName:String,start:Int,end:Int)
case class Gene(contigName:String,start:Int,end:Int,geneId:String,strand:String)

class FeatureCountsTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext{


  before{
    System.setSecurityManager(null)
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
  }

  test ("Feature counts for chr1:20138-20294") {
    val query ="""SELECT count(*),targets.contigName,targets.start,targets.end
              FROM reads JOIN targets
        |ON (
        |  targets.contigName=reads.contigName
        |  AND
        |  reads.end >= targets.start
        |  AND
        |  reads.start <= targets.end
        |)
        |GROUP BY targets.contigName,targets.start,targets.end
        |having contigName='chr1' AND    start=20138 AND  end=20294""".stripMargin


    spark
      .sparkContext
      .hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)

    val alignments = spark
      .sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](getClass.getResource("/NA12878.slice.bam").getPath)
      .map(_._2.get)
      .map(r => Region(r.getContig, r.getStart, r.getEnd))

    val reads = spark
      .sqlContext
      .createDataFrame(alignments)
    reads
      .createOrReplaceTempView("reads")

    val targets = spark
      .sqlContext
      .createDataFrame(Array(Region("chr1",20138,20294)))
    targets
      .createOrReplaceTempView("targets")

    spark.sql(query).explain(false)
    assert(spark.sql(query).first().getLong(0) === 1484L)

  }



}

