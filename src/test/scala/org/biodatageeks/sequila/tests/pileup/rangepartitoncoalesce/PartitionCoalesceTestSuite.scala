package org.biodatageeks.sequila.tests.pileup.rangepartitoncoalesce

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.pileup.Pileup
import org.biodatageeks.sequila.pileup.partitioning.PartitionUtils
import org.biodatageeks.sequila.tests.pileup.PileupTestBase
import org.biodatageeks.sequila.utils.{InternalParams, SequilaRegister}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

class PartitionCoalesceTestSuite extends PileupTestBase{

  test("Basic count"){
    val splitSize = "1000000"
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    spark.sparkContext.setLogLevel("ERROR")
    val ss = SequilaSession(spark)

    SequilaRegister.register(ss)
    val pileup = new Pileup[BAMBDGInputFormat](spark)
    val allAlignments = pileup.readTableFile(name=tableName, sampleId)
    val partLowerBounds = PartitionUtils.getPartitionLowerBound(allAlignments)
    partLowerBounds.foreach(p => println(s"${p.idx} -> ${p.record.getReadName}") )
    println(allAlignments.filter(_.getReadName=="61CC3AAXX100125:7:66:10690:16356").collect()(0).getEnd)
    /**
      * Base Partitions bounds
      *     0 : chrM, 7 <-> chrM, 7889
      *     1 : chrM, 7814 <-> chrM, 14322
      *     2 : chrM, 14247 <-> chr1, 10036
      */
    //println(allAlignments.getNumPartitions)


  }
}
