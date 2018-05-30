

SeQuiLa API
===========


The API that is provided by SeQuiLa is not a set of predefined functions but
rather it  exposes a flexible and enhanced SQL interface for querying genomic datasets.
Enhanced in terms  range joins performance between datasets which is crucial to data analysis in bioinformatics
Below we will describe how to implement SQL queries to efficiently join genomic datasets with SeQuiLa.




General usage
##############
Let's assume that you already have prepared or imported two datasets ``s1`` and ``s2`` which both
contain location indices of start and end of interval (usually named: start, end).
Optionally you may also want to take chromosome name into account (usually chr).

You have to register SeQuiLa extra strategy and prepare a SQL query that will be performed.

Find overlaps - basic
***********************

Range join on interval on two datasets can be defined in SQL.
You may want to select all resulting columns ```select * ``` or just a subset of them.
As a result of executing this query by Spark SQL you will another dataset

:: 

   val sqlQuery = 
      s"""
        |SELECT * 
        |FROM s1 JOIN s2 
        |ON (end1>=start1 and start1<=end2 )
        """
   val res = spark.sql(query)

This result can be then used for various data manipulations and for further pipeline implementation.


Find overlaps within chromosomes
*********************************

If your dataset contains chromosomes information as well
you may want to add an additional constraint to join condition so as to find only overlaps from the same chromosome.

::

      val sqlQuery = 
        s"""
        |SELECT * 
        |FROM s1 JOIN s2 
        |ON (s1.chr=s2.chr and s1.end>=s2.start and s1.start<=s2.end )
        """
     val res = spark.sql(query)


Using builtin data sources for BAM and ADAM file formats
########################################################

SeQuiLa introduces native BAM/ADAM data source that enables user to create a view over the exiting files to
process and query them using a SQL interface:

.. code-block:: scala

    val tableNameBAM = "reads"
    spark.sql("CREATE DATABASE BDGEEK")
    spark.sql("USE BDGEEK")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "/data/input/multisample/*.bam")
         |
      """.stripMargin)
    spark.sql("SELECT sampleId,contigName,start,end,cigar FROM reads").show(5)

Implicit partition pruning for BAM data source
##############################################

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


If you run the above query you should get the information that SeQuiLa optimized the physical plan  and will only read 2 BAM files
instead of 3 to answer your query:

.. code-block:: bash

    WARN BAMRelation: Partition pruning detected,reading only files for samples: NA12878,NA12879


Using UDFs
##########

SeQuiLa introduces several UserDefinedFunctions. 

shift
******

Shift function is performing operation of shifting the ranges by
a specified number of positions. To use the function within query it needs to be registered. Sample query using shift function:

::

   spark.sqlContext.udf.register("shift", RangeMethods.shift _)

    val query =
     s"""
       |SELECT start, end, shift(start,end,5) as shiftedInterval FROM ref
      """.stripMargin

It returns range with start and end fields.

.. highlight:: console

::

    |-- start: integer (nullable = true)
    |-- end: integer (nullable = true)
    |-- shiftedInterval: struct (nullable = true)
    |    |-- start: integer (nullable = false)
    |    |-- end: integer (nullable = false)

.. highlight:: console

resize
*******

Resize function is performing operation of extending the range by specified width.
It returns range with start and end fields. Sample query using resize function:

::

   spark.sqlContext.udf.register("resize", RangeMethods.resize _)

    val query =
     s"""
        |SELECT start, end, resize(start,end,5,"center") as resizedInterval FROM ref
      """.stripMargin

calcOverlap
************

calcOverlap function returns the width of overlap between intervals.
To use the function within query it needs to be registered. Sample query using overlaplength function:

::

   spark.sqlContext.udf.register("overlaplength", RangeMethods.calcOverlap _)

   val query =
     s"""
       |SELECT * FROM reads JOIN targets
       |ON (targets.contigName=reads.contigName
       |AND
       |reads.end >=targets.start
       |AND
       |reads.start<= targets.end
       | AND
       |overlaplength(reads.start,reads.end,targets.start,targets.end)>=10
       |)
       |
         """.stripMargin

flank
*******

Flank function is performing operation of calculating the flanking range with specified width. F
irst boolean argument indicates whether flanking should be performed from start of range (true) or end (false).
Second boolean argument set to true indicates that flanking range should contain not only outside of original range, but also inside.
In that case width of flanking range is doubled. Flank function returns range with start and end fields. Sample query using flank function:

::

   spark.sqlContext.udf.register("flank", RangeMethods.flank _)

    val query =
      s"""
        |SELECT start, end, flank(start,end,5,true,true) as flankedInterval FROM ref
       """.stripMargin
   
promoters
*********

Promoters function is performing operation of calculating promoter for the range with given upstream and downstream.
It returns range with start and end fields. Sample query using promoters function:

::

    spark.sqlContext.udf.register("promoters", RangeMethods.promoters _)

    val query =
      s"""
        |SELECT start, end, promoters(start,end,100,20) as promoterInterval FROM ref
       """.stripMargin

reflect
*******

Reflect function is performing operation of reversing the range relative to specified reference bounds.
It returns range with start and end fields. Sample query using reflect function:

::

    spark.sqlContext.udf.register("reflect", RangeMethods.reflect _)

    val query =
      s"""
        |SELECT start, end, reflect(start,end,11000,15000) as reflectedInterval FROM ref
       """.stripMargin 
   
   
Additional parameteres
######################

Currently SeQuiLa provides three additional parameters that impact joining in terms of results and speed of execution


minOverlap
***********
This parameter is defining the minimal overlapping positions for interval.
The default value is set to 1, meaning that two intervals are considered overlapping if they have at least one position in common.

Parameter is set via configuration:
::
   
   spark.sqlContext.setConf("minOverlap","5")



maxGap
*******

This parameter is defining possible separation of intervals of maxGap or less and still consider them as overlapping. The default is equal to 0.

Parameter is set via configuration:
::

   spark.sqlContext.setConf("maxGap","10")



maxBroadcastSize
*****************
This parameter is defining the decision boundary for choosing to broadcast whole table (with all columns) to the tree (peered for narrow dataframes)
or just intervals (preferred for wider dataframes). If the whole table is broadcasted the solution
is more memory-demanding but joining happens in one step. If just intervals are broadcast joining happens in two steps.

By default the parameter is set to 10240 kB

Parameter is set via coniguration:
::

   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (10*(1024*1024)).toString)


useJoinOrder
**************
If this parameter is set to FALSE the algorithm itself decides which table is used for broadcasting.
It performs row counting on both tables and chooses smaller one.

To achieve even better performance you can set this parameter to TRUE.
In this case, the algorithm does not check table sizes but blindly broadcasts the second table.
You should use this parameter if you know table sizes beforehand

By default the parameter is set to false.

Parameter is set via coniguration:
::

   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder", "true")


Coverage
##########

In order to compute coverage for your sample you can run a set of queries as follows:

.. code-block:: scala

    val tableNameBAM = "reads"
    val bamPath = "/data/samples/*.bam"
    ss.sql("CREATE DATABASE dna")
    ss.sql("USE dna")
    ss.sql(
            s"""
               |CREATE TABLE ${tableNameBAM}
               |USING org.biodatageeks.datasources.BAM.BAMDataSource
               |OPTIONS(path "${bamPath}")
               |
          """.stripMargin)
    ss.sql(s"SELECT * FROM coverage('${tableNameBAM}')").show(5)

    +--------+----------+--------+--------+
    |sampleId|contigName|position|coverage|
    +--------+----------+--------+--------+
    | NA12878|      chr1|     137|       1|
    | NA12878|      chr1|     138|       1|
    | NA12878|      chr1|     139|       1|
    | NA12878|      chr1|     140|       1|
    | NA12878|      chr1|     141|       1|
    +--------+----------+--------+--------+

If you would like to do additional short reads prefiltering, you can create a temporary table and use it as an input to the coverage function, e.g.:

.. code-block:: scala

    ss.sql(s"CREATE TABLE filtered_reads AS SELECT * FROM ${tableNameBAM} WHERE mapq > 10 AND start> 200")
    ss.sql(s"SELECT * FROM coverage('filtered_reads')").show(5)

    +--------+----------+--------+--------+
    |sampleId|contigName|position|coverage|
    +--------+----------+--------+--------+
    | NA12878|      chr1|     361|       1|
    | NA12878|      chr1|     362|       1|
    | NA12878|      chr1|     363|       1|
    | NA12878|      chr1|     364|       1|
    | NA12878|      chr1|     365|       1|
    +--------+----------+--------+--------+

(Experimental WIP) If you are interested in coverage histograms using e.g. mapping quality you can use the following table valued function:

.. code-block:: scala

    ss.sql(s"SELECT * FROM coverage_hist('${tableNameBAM}') WHERE position=20204").show()

    +--------+----------+--------+------------------+-------------+
    |sampleId|contigName|position|          coverage|coverageTotal|
    +--------+----------+--------+------------------+-------------+
    | NA12878|      chr1|   20204|[1017, 0, 2, 0, 0]|         1019|
    +--------+----------+--------+------------------+-------------+


