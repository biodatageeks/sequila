package org.biodatageeks.sequila.pileup

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.orc.TypeDescription
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcOutputFormat
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.biodatageeks.sequila.pileup.partitioning.LowerPartitionBoundAlignmentRecord
import org.biodatageeks.sequila.utils.SequilaRegister
import org.seqdoop.hadoop_bam.{BAMInputFormat, CRAMBDGInputFormat, CRAMInputFormat, SAMRecordWritable}


case class Contig(contig: String)
object PileupDebugger {

  def main(args: Array[String]): Unit = {
    System.setSecurityManager(null)
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.driver.memory","16g")
      .getOrCreate()

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bamPath = "/Users/mwiewior/research/data/WES/NA12878.proper.wes.md.bam"
    val referencePath = "/Users/mwiewior/research/data/Homo_sapiens_assembly18.fasta"

    spark
      .sparkContext
      .hadoopConfiguration
      .set("hadoopbam.cram.reference-source-path", referencePath)
    ss
      .sparkContext
      .hadoopConfiguration
      .setInt("mapred.min.split.size", 2*67108864)


    val conf = new Configuration()
    conf.set("orc.mapred.output.schema", "struct<contig:string>")
    conf.set("orc.compress", "SNAPPY")
    val rdd =spark.sparkContext
      .newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](bamPath)
      .mapPartitions{
        val schema = TypeDescription.fromString("struct<contig:string>")
        iterator => {
          iterator
            .filter(_._2.get().getContig != null)
            .map( r => (NullWritable.get(),
            {
              val record = OrcStruct.createValue(schema).asInstanceOf[OrcStruct]
              record
                .setFieldValue("contig", new Text(r._2.get().getContig))
              record
            }))
        }
      }
  }

}
