Data formats
############

BAM/CRAM
********

Using builtin data sources for BAM and CRAM file formats
========================================================

SeQuiLa introduces native BAM/CRAM data sources that enable user to create a view over the existing files to
process and query them using a SQL interface:

.. code-block:: scala

    val tableNameBAM = "reads"
    ss.sql("CREATE DATABASE BDGEEK")
    ss.sql("USE BDGEEK")
    ss.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "/data/input/multisample/*.bam")
         |
      """.stripMargin)
    ss.sql("SELECT sampleId,contigName,start,end,cigar FROM reads").show(5)


The BAM file can contain aligned short reads or long reads. The structure and syntax of the analyses does not depend on the lenght of reads.

.. code-block:: scala

    val tableNameBAM = "reads"
    ss.sql("CREATE DATABASE BDGEEK")
    ss.sql("USE BDGEEK")
    ss.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "/data/input/longs/rel5-guppy-0.3.0-chunk10k.sorted.bam.bam")
         |
      """.stripMargin)
    ss.sql("SELECT sampleId,contigName,start,end,cigar FROM reads").show(5)




In case of CRAM file format you need to specify both globbed path to the CRAM files as well as path to the reference file (both *.fa and *.fa.fai)
files should stored together in the same folder, e.g.:

.. code-block:: scala

    val tableNameCRAM = "reads_cram"
    val cramPath = getClass.getResource("/data/input/test*.cram").getPath
    val refPath = getClass.getResource("/phix-illumina.fa").getPath
    ss.sql("CREATE DATABASE BDGEEK")
    ss.sql("USE BDGEEK")
    spark.sql(
        s"""
           |CREATE TABLE ${tableNameCRAM}
           |USING org.biodatageeks.sequila.datasources.BAM.CRAMDataSource
           |OPTIONS(path "${cramPath}", refPath "${refPath}")
           |
      """.stripMargin)
    ss.sql("SELECT sampleId,contigName,start,end,cigar FROM reads_cram").show(5)




CTaS/IaS for BAM file format
========================================================

Create table as select
----------------------

.. code-block:: scala

    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss
      .sql(
        s"""
          |CREATE TABLE IF NOT EXISTS bam_ctas USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
          |OPTIONS(path "/data/ctas/*.bam")
          |AS SELECT * FROM reads WHERE sampleId='NA12878'
        """.stripMargin)
          .explain

Explain plan:

.. code-block:: bash

    == Physical Plan ==
    ExecutedCommand
   +- CreateBAMDataSourceTableAsSelectCommand `bam_ctas`, Ignore
         +- 'Project [*]
            +- 'Filter ('sampleId = NA12878)
               +- SubqueryAlias reads
                  +- Relation[sampleId#2,contigName#3,start#4,end#5,cigar#6,mapq#7,baseq#8,reference#9,flags#10,materefind#11,SAMRecord#12] org.biodatageeks.sequila.datasources.BAM.BDGAlignmentRelation@14b3ba01


Insert table as select
----------------------

.. code-block:: scala

    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss
      .sql(s"INSERT INTO bam_ias SELECT * FROM reads WHERE sampleId='NA12878'")
      .explain

Explain plan:

.. code-block:: bash

   == Physical Plan ==
    ExecutedCommand
   +- InsertIntoBAMDataSourceCommand Relation[sampleId#121,contigName#122,start#123,end#124,cigar#125,mapq#126,baseq#127,reference#128,flags#129,materefind#130,SAMRecord#131] org.biodatageeks.sequila.datasources.BAM.BDGAlignmentRelation@60fd33fe, false, reads
         +- 'Project [*]
            +- 'Filter ('sampleId = NA12878)
             +- SubqueryAlias reads
               +- Relation[sampleId#110,contigName#111,start#112,end#113,cigar#114,mapq#115,baseq#116,reference#117,flags#118,materefind#119,SAMRecord#120] org.biodatageeks.sequila.datasources.BAM.BDGAlignmentRelation@765fc5be18

Insert overwrite table as select
--------------------------------
This operation overwrites not the whole table but only records for a specified sample (e.g. NA12878).

.. code-block:: scala

    val  ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss
      .sql(s"INSERT OVERWRITE TABLE bam_ias SELECT * FROM reads sampleId='NA12878' limit 10")
      .explain

Explain plan:

.. code-block:: bash

  == Physical Plan ==
    ExecutedCommand
   +- InsertIntoBAMDataSourceCommand Relation[sampleId#228,contigName#229,start#230,end#231,cigar#232,mapq#233,baseq#234,reference#235,flags#236,materefind#237,SAMRecord#238] org.biodatageeks.sequila.datasources.BAM.BDGAlignmentRelation@2afa03c8, true, reads
         +- 'GlobalLimit 10
            +- 'LocalLimit 10
               +- 'Project [*]
                  +- SubqueryAlias reads
                   +- 'Filter ('sampleId = NA12878)
                     +- Relation[sampleId#217,contigName#218,start#219,end#220,cigar#221,mapq#222,baseq#223,reference#224,flags#225,materefind#226,SAMRecord#227] org.biodatageeks.sequila.datasources.BAM.BDGAlignmentRelation@140ae9941

Implicit partition pruning for BAM data source
========================================================

BAM data source supports implicit `partition pruning <https://docs.oracle.com/database/121/VLDBG/GUID-E677C85E-C5E3-4927-B3DF-684007A7B05D.htm#VLDBG00401>`_
mechanism to speed up queries that are restricted to only subset of samples from a table. Consider a following example:

.. code-block:: bash

    MacBook-Pro:multisample marek$ ls -ltr
    total 2136
    -rw-r--r--  1 marek  staff  364043 May 15 18:53 NA12877.slice.bam
    -rw-r--r--  1 marek  staff  364043 May 15 18:53 NA12878.slice.bam
    -rw-r--r--  1 marek  staff  364043 May 15 18:53 NA12879.slice.bam

    MacBook-Pro:multisample marek$ pwd
    /Users/marek/data/multisample


.. code-block:: scala

    import org.apache.spark.sql.{SequilaSession, SparkSession}
    val bamPath ="/Users/marek/data/multisample/*.bam"
    val tableNameBAM = "reads"
    val ss: SparkSession = SequilaSession(spark)
     ss.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)

    val query =
      """
        |SELECT sampleId,count(*) FROM reads where sampleId IN('NA12878','NA12879')
        |GROUP BY sampleId order by sampleId
      """.stripMargin
     ss.sql(query)


If you run the above query you should get the information that SeQuiLa optimized the physical execution plan  and will only read 2 BAM files
instead of 3 to answer your query:

.. code-block:: bash

    WARN BAMRelation: Partition pruning detected,reading only files for samples: NA12878,NA12879


Speeding up interval queries in BAM data source using an index (BAI)
====================================================================

SeQuiLa can take advantage of the existing BAI index to speed up interval queries using: `contigName`, `start`, end `fields`.

First of all, make sure that you have both BAM and BAI files in the same folder (you can have more than one pair BAM/BAI) :

.. code-block:: bash

    MacBook-Pro:bdg-sequila marek$ ls -ltr /data/NA12878.ga2.*bam*
    -rw-r--r--@ 1 marek  staff  17924580574 Mar 14 20:21 NA12878.ga2.exome.maq.recal.bam
    -rw-r--r--  1 marek  staff      4022144 Jul  8 16:47 NA12878.ga2.exome.maq.recal.bam.bai

Then you can create a new SeQuiLa table over this folder:

.. code-block:: scala

    import org.apache.spark.sql.SequilaSession
    import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister}

    val ss = SequilaSession(spark)
    /*inject bdg-granges strategy*/
    SequilaRegister.register(ss)

    ss.sql("""
    CREATE TABLE reads_exome USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
    OPTIONS(path '/data/NA12878.ga2.exome.*.bam')
    """.stripMargin)

First time we run the query without pushing genomic intervals predicates:

.. code-block:: scala

    spark.time{
     ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","false")
      ss.sql("SELECT count(*) FROM reads_exome WHERE contigName='chr1' AND start=20138").show
    }


.. code-block:: bash

    18/07/25 12:57:44 WARN BAMRelation: GRanges: chr1:20138-20138, false
    +--------+
    |count(1)|
    +--------+
    |      20|
    +--------+

    Time taken: 186045 ms


Now we rerun the query with pushing the predicates:

.. code-block:: scala

    spark.time{
      ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","true")
      ss.sql("SELECT count(*) FROM reads_exome WHERE contigName='chr1' AND start=20138").show
    }

.. code-block:: bash

    18/07/25 13:01:40 WARN BAMRelation: GRanges: chr1:20138-20138, true
    18/07/25 13:01:40 WARN BAMRelation: Interval query detected and predicate pushdown enabled, trying to do predicate pushdown using intervals chr1:20138-20138
    +--------+
    |count(1)|
    +--------+
    |      20|
    +--------+

    Time taken: 732 ms


Genomic intervals are also supported:

.. code-block:: scala

     spark.time{
     ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","false")
      ss.sql("""SELECT count(*) FROM reads_exome
      WHERE contigName='chr1' AND start >= 1996 AND end <= 2071""".stripMargin).show
    }

.. code-block:: bash

    18/07/25 17:52:05 WARN BAMRelation: GRanges: chr1:1996-2071, false
    +--------+
    |count(1)|
    +--------+
    |       3|
    +--------+

    Time taken: 147638 ms


.. code-block:: scala

    spark.time{
     ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","true")
      ss.sql("""SELECT count(*) FROM reads_exome
      WHERE contigName='chr1' AND start >= 1996 AND end <= 2071""".stripMargin).show
    }

.. code-block:: bash

    18/07/25 17:55:05 WARN BAMRelation: GRanges: chr1:1996-2071, true
    18/07/25 17:55:05 WARN BAMRelation: Interval query detected and predicate pushdown enabled, trying to do predicate pushdown using intervals chr1:1996-2071
    +--------+
    |count(1)|
    +--------+
    |       3|
    +--------+

    Time taken: 401 ms


Speeding up BAM scans using Intel Genomics Kernel Library's Inflater
====================================================================

SeQuiLa starting from version 0.4.1 supports `Genomics Kernel Library (GKL)
<https://github.com/Intel-HLS/GKL>`_ on Mac OS X and Linux platforms for speeding up BAM blocks decompression.
In order to start using optimized Intel inflater library you need simply to set the following parameter:

.. code-block:: scala

    import org.apache.spark.sql.{SequilaSession, SparkSession}
    import org.biodatageeks.sequila.utils.SequilaRegister
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sql(...)

Swappable alignment file reading mechanism
====================================================================

SeQuiLa support two methods of reading alignment files (BAM/CRAM).
It uses hadoopBAM library by default but it can be changed to disq by using ``spark.biodatageeks.readAligment.method`` parameter as follows:

.. code-block:: scala

    import org.apache.spark.sql.{SequilaSession, SparkSession}
    import org.biodatageeks.sequila.utils.SequilaRegister
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.readAligment.method","disq")

ADAM
****
ADAM data source can be defined in the analogues way (just requires using org.biodatageeks.sequila.datasources.ADAM.ADAMDataSource), e.g. :

.. code-block:: scala

    val tableNameADAM = "reads_adam"
    ss.sql("CREATE DATABASE BDGEEK")
    ss.sql("USE BDGEEK")
    ss.sql(
      s"""
         |CREATE TABLE ${tableNameADAM}
         |USING org.biodatageeks.sequila.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "/data/input/multisample/*.adam")
         |
      """.stripMargin)
    ss.sql("SELECT sampleId,contigName,start,end,cigar FROM reads_adam").show(5)



BED
****
Coverage information can be exported to standard BED format. Actually, calculated data can be stored in any kind of text file (csv, tsv etc). Example export command 

.. code-block:: scala

    val tableNameADAM = "reads_adam"
    ss.sql("CREATE DATABASE BDGEEK")
    ss.sql("USE BDGEEK")
    ss.sql(
      s"""
         |CREATE TABLE ${tableNameADAM}
         |USING org.biodatageeks.sequila.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "/data/input/multisample/*.bam")
         |
      """.stripMargin)
    val cov = ss.sql("SELECT * FROM coverage('${tableNameBAM}','NA12878', 'blocks')")
    cov.coalesce(1).write.mode("overwrite").option("delimiter", "\t").csv("/data/output/cov.bed")


VCF
****


.. code-block:: scala

    val tableNameVCF = "test_vcf"
    ss.sql("CREATE DATABASE BDGEEK")
    ss.sql("USE BDGEEK")
    ss.sql(
      s"""
         |CREATE TABLE ${tableNameVCF}
         |USING org.biodatageeks.sequila.datasources.VCF.VCFDataSource
         |OPTIONS(path "/data/input/vcf/*.vcf")
         |
      """.stripMargin)
   ss.sql(s"SELECT * FROM ${tableNameVCF} LIMIT 5").show(false)

    +------+-----+-----+-----+---------+---+---+----+------+--------------------+---+---+---+-----+---------+
    |contig|  pos|start| stop|       id|ref|alt|qual|filter|                info| gt| gq| dp|   hq|sample_id|
    +------+-----+-----+-----+---------+---+---+----+------+--------------------+---+---+---+-----+---------+
    |    20|14370|14369|14370|rs6054257|  G|  A|  29|  PASS|[ns -> 3, db -> D...|0|0| 48|  1|51,51|  NA00001|
    |    20|14370|14369|14370|rs6054257|  G|  A|  29|  PASS|[ns -> 3, db -> D...|1|0| 48|  8|51,51|  NA00002|
    |    20|14370|14369|14370|rs6054257|  G|  A|  29|  PASS|[ns -> 3, db -> D...|1/1| 43|  5|  .,.|  NA00003|
    |    20|17330|17329|17330|        .|  T|  A|   3|   q10|[ns -> 3, dp -> 1...|0|0| 49|  3|58,50|  NA00001|
    |    20|17330|17329|17330|        .|  T|  A|   3|   q10|[ns -> 3, dp -> 1...|0|1|  3|  5| 65,3|  NA00002|
    +------+-----+-----+-----+---------+---+---+----+------+--------------------+---+---+---+-----+---------+

