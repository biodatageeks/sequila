package org.biodatageeks.sequila.tests.pileup.rangepartitoncoalesce

import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.datasources.BAM.BAMTableReader
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.pileup.Pileup
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.tests.pileup.PileupTestBase
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat
import org.biodatageeks.sequila.pileup.model.AlignmentsRDDOperations.implicits._

case class AlignmentReadId(name: String, flag:Int)
class PartitionCoalesceTestSuite extends PileupTestBase with RDDComparisons {

  val query =
    s"""
       |SELECT *
       |FROM  pileup('${tableName}', '${sampleId}', '${referencePath}', false)
               """.stripMargin

  test("Check if last read of the partition is found correctly"){

    /**
      * Base Partitions bounds
      *     0 : chrM, 7 <-> chrM, 7889
      *     1 : chrM, 7831 <-> chrM, 14322
      *     2 : chrM, 14247 <-> chr1, 10036
      *
      *     First reads of parition with id:
      *     0 -> 61DC0AAXX100127:8:58:8295:16397
            1 -> 61CC3AAXX100125:6:102:19312:9444
            2 -> 61CC3AAXX100125:6:36:1256:17370

      */
    Array("hadoopBAM", "disq").foreach ( m => {
      val splitSize = "1000000"
      val ss = SequilaSession(spark)
      SequilaRegister.register(ss)
      ss
        .sqlContext
        .setConf(InternalParams.IOReadAlignmentMethod, m)
      ss
        .sparkContext
        .hadoopConfiguration
        .setInt("mapred.max.split.size", splitSize.toInt)

      val tableReader = new BAMTableReader[BAMBDGInputFormat](ss, tableName, sampleId, "bam", None)
      val conf = new Conf

      val allAlignments = tableReader.readFile
      val lowerBounds = allAlignments.getPartitionLowerBound
      val adjBounds = allAlignments.getPartitionBounds(tableReader.asInstanceOf[BAMTableReader[BDGAlignInputFormat]], conf, lowerBounds)

      assert(adjBounds(0).readName.get == "61DC0AAXX100127:8:61:5362:15864") //max pos read of partition 0
      assert(adjBounds(1).readName.get == "61DC0AAXX100127:8:58:2296:9811") //max pos read of partition 1
      }
    )
  }

  test("Basic count"){
    val splitSize = "1000000"
    val allReadsPath: String = getClass.getResource("/partitioner/read_names.txt.bz2").getPath

    Array("hadoopBAM", "disq").foreach ( m => {
        val ss = SequilaSession(spark)
        SequilaRegister.register(ss)
        ss
          .sqlContext
          .setConf(InternalParams.IOReadAlignmentMethod, m)
        ss
          .sparkContext
          .hadoopConfiguration
          .setInt("mapred.max.split.size", splitSize.toInt)
        val conf = new Conf
        val tableReader = new BAMTableReader[BAMBDGInputFormat](spark, tableName, sampleId, "bam", None)

        val allAlignments = tableReader.readFile
        allAlignments.getPartitionLowerBound.foreach(r => println(r.record.getReadName))

        allAlignments.foreachPartition(r => println(r.toArray.length))
        val (repartitionedAlignments, bounds) = allAlignments.repartition(tableReader.asInstanceOf[BAMTableReader[BDGAlignInputFormat]], conf)
        val testReads = repartitionedAlignments
          .map(r => AlignmentReadId(r.getReadName, r.getFlags))
          .distinct()

        /**
          * ** check distinct count reads
          * samtools view src/test/resources/multichrom/mdbam/NA12878.multichrom.md.bam| wc -l
          *  22607
          */
        assert(testReads.count() == 22607) // check distinct count reads

        val allReadsRDD = spark.sparkContext.textFile(allReadsPath)
          .map(_.split("\\t"))
          .map(r => AlignmentReadId(r(0), r(1).toInt))

        /**
          * check if all reads are there tuple( readName, flag)
          */
        assertRDDEquals(testReads, allReadsRDD)
      }
    )
  }
}
