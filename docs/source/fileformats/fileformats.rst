Data input formats
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
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "/data/input/multisample/*.bam")
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
           |USING org.biodatageeks.datasources.BAM.CRAMDataSource
           |OPTIONS(path "${cramPath}", refPath "${refPath}")
           |
      """.stripMargin)
    ss.sql("SELECT sampleId,contigName,start,end,cigar FROM reads_cram").show(5)


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
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
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
    import org.biodatageeks.utils.{SequilaRegister, UDFRegister}

    val ss = SequilaSession(spark)
    /*inject bdg-granges strategy*/
    SequilaRegister.register(ss)

    ss.sql("""
    CREATE TABLE reads_exome USING org.biodatageeks.datasources.BAM.BAMDataSource
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
    import org.biodatageeks.utils.SequilaRegister
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sql(...)



ADAM
****
ADAM data source can be defined in the analogues way (just requires using org.biodatageeks.datasources.ADAM.ADAMDataSource), e.g. :

.. code-block:: scala

    val tableNameADAM = "reads_adam"
    ss.sql("CREATE DATABASE BDGEEK")
    ss.sql("USE BDGEEK")
    ss.sql(
      s"""
         |CREATE TABLE ${tableNameADAM}
         |USING org.biodatageeks.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "/data/input/multisample/*.adam")
         |
      """.stripMargin)
    ss.sql("SELECT sampleId,contigName,start,end,cigar FROM reads_adam").show(5)