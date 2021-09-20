package org.biodatageeks.sequila.tests.pileup.rangepartitoncoalesce

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.pileup.Pileup
import org.biodatageeks.sequila.pileup.partitioning.PartitionUtils
import org.biodatageeks.sequila.tests.pileup.PileupTestBase
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

class PartitionCoalesceTestSuite extends PileupTestBase{

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
    val splitSize = "1000000"
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
//    spark.sparkContext.setLogLevel("INFO")
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val pileup = new Pileup[BAMBDGInputFormat](ss)
    val allAlignments = pileup.readTableFile(name=tableName, sampleId)
    val adjBounds = pileup.getPartitionBounds(allAlignments,tableName, sampleId)

    assert(adjBounds(0).readName.get == "61CC3AAXX100125:5:66:10346:21333") //last read of partition 0
    assert(adjBounds(1).readName.get == "61CC3AAXX100125:5:62:5183:2612") //last read of partition 1
  }

  test("Basic count"){
    val splitSize = "1000000"
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
//    spark.sparkContext.setLogLevel("INFO")
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    val pileup = new Pileup[BAMBDGInputFormat](ss)
    val allAlignments = pileup.readTableFile(name=tableName, sampleId).filter(r => r.getReadUnmappedFlag != true)

    PartitionUtils.getPartitionLowerBound(allAlignments).foreach(r => println(r.record.getReadName))

    allAlignments.foreachPartition(r => println(r.toArray.length) )
    val repartitionedAlignments = pileup.repartitionAlignments(allAlignments, tableName, sampleId)
    repartitionedAlignments.foreachPartition(r => println(r.toArray.length) )
    println(repartitionedAlignments.count())


  }
}
