

API
=======


The API that is provided by SeQuiLa is not a set of predefined functions but
rather it  exposes a flexible and enhanced SQL interface for querying genomic datasets.
Enhanced in terms of high  performance of range joins between datasets, which is crucial to data analysis in bioinformatics.
Below we will describe how to implement SQL queries to efficiently join genomic datasets with SeQuiLa.




Dataset definition 
###################

Let's assume that you already have prepared or imported two datasets ``s1`` and ``s2`` which both
contain location indices of start and end of interval (usually named: start, end).
You may also want to take chromosome name into account (usually chr). Additionally your genomic intervals dataset can contain optional annotations. 


.. figure:: structure.*
    :scale: 100

    Sample datasets' structure. Each dataset must contain genomic interval coordinates (chromosome, start, stop). Additionally it may, but doesn't have to, contain columns with genomic interval annotations.


You have to register SeQuiLa extra strategy and prepare a SQL query that will be executed.

Operations
############

Find overlaps 
***********************

Range join on interval on two datasets can be defined in SQL.
You may want to select all resulting columns ```select * ``` or just a subset of them.
As a result of executing this query by Spark SQL you will get another dataset/dataframe.

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



Using UDFs
##########

SeQuiLa introduces several useful UserDefinedFunctions.

If using in Scala outside bdg-shell then you need first register the UDFs as follows:

.. code-block:: scala

    import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
    import org.apache.spark.sql.SequilaSession
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
        .getOrCreate()
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    UDFRegister.register(ss)


Then using them is straightforward:

.. code-block:: scala

    val query = "SELECT ..."
    ss
    .sql(query)
    .show

bdg_shift
*********

Shift function performs an operation of shifting ranges by
a specified number of positions. A sample query using the shift function:

::

    val query =
     s"""
       |SELECT start, end, bdg_shift(start,end,5) as shiftedInterval FROM ref
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

bdg_resize
**********

Resize function performs an operation of extending the range by a specified width.
It returns range with start and end fields. A sample query using the resize function:

::

    val query =
     s"""
        |SELECT start, end, bdg_resize(start,end,5,"center") as resizedInterval FROM ref
      """.stripMargin

bdg_overlaplength
*****************

calcOverlap function returns the width of overlap between intervals.
A sample query using the overlaplength function:

::

   val query =
     s"""
       |SELECT * FROM reads JOIN targets
       |ON (targets.contigName=reads.contigName
       |AND
       |reads.end >=targets.start
       |AND
       |reads.start<= targets.end
       | AND
       |bdg_overlaplength(reads.start,reads.end,targets.start,targets.end)>=10
       |)
       |
         """.stripMargin

bdg_flank
*********

Flank function performs an operation of calculating the flanking range with specified width. The first boolean argument indicates whether flanking should be performed from start of range (true) or end (false).
The second boolean argument set to true indicates that flanking range should contain not only outside of original range, but also inside.
In that case width of flanking range is doubled. Flank function returns range with start and end fields. A sample query using the flank function:

::

    val query =
      s"""
        |SELECT start, end, bdg_flank(start,end,5,true,true) as flankedInterval FROM ref
       """.stripMargin
   
bdg_promoters
*************

Promoters function performs an operation of calculating promoter for the range with given upstream and downstream.
It returns range with start and end fields. A sample query using the promoters function:

::

    val query =
      s"""
        |SELECT start, end, bdg_promoters(start,end,100,20) as promoterInterval FROM ref
       """.stripMargin

bdg_reflect
***********

Reflect function performs and operation of reversing the range relative to specified reference bounds.
It returns range with start and end fields. A sample query using the reflect function:

::

    val query =
      s"""
        |SELECT start, end, bdg_reflect(start,end,11000,15000) as reflectedInterval FROM ref
       """.stripMargin 
   
   
bdg_coverage
************

In order to compute coverage for a sample one can run a set of queries as follows:

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
    ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878')").show(5)

            +----------+-----+---+--------+
            |contigName|start|end|coverage|
            +----------+-----+---+--------+
            |      chr1|   34| 34|       1|
            |      chr1|   35| 35|       2|
            |      chr1|   36| 37|       3|
            |      chr1|   38| 40|       4|
            |      chr1|   41| 49|       5|
            +----------+-----+---+--------+


Functional parameteres
######################



minOverlap
***********
This parameter defines the minimal overlapping positions for interval.
The default value is set to 1, meaning that two intervals are considered as overlapping if they have at least one position in common.

Parameter can be set in the following way:
::
   
   spark.sqlContext.setConf("minOverlap","5")



maxGap
*******

This parameter defines possible separation of intervals of maxGap or less and still consider them as overlapping. The default is equal to 0.

Parameter can be set in the following way:
::

   spark.sqlContext.setConf("maxGap","10")



Performance tuning parameters
###############################

maxBroadcastSize
*****************
This parameter defines the threshold for the decision whether to broadcast whole table (with all columns referenced in a query) to the tree (preferred smaller dataframes)
or just intervals (preferred for very large dataframes). If the whole table is broadcasted the solution
is more memory-demanding but joining happens in one step. If just intervals are broadcast joining happens in two steps.

By default the parameter is set to 10240 kB

Parameter can be set in the following way:
::

   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (10*(1024*1024)).toString)


useJoinOrder
**************
If this parameter is set to FALSE the algorithm itself decides which table is used for broadcasting.
It performs row counting on both tables and chooses smaller one.

To achieve even better performance you can set this parameter to TRUE.
In this case, the algorithm does not check table sizes but blindly broadcasts the second table.
You should use this parameter if you know approx. table sizes beforehand.

By default the parameter is set to false.

Parameter can be set in the following way:
::

   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder", "true")




BAM/ADAM
#########

Using builtin data sources for BAM and ADAM file formats
**********************************************************

SeQuiLa introduces native BAM/ADAM data sources that enable user to create a view over the existing files to
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

ADAM data source can be defined in the analogues way (just requires using org.biodatageeks.datasources.ADAM.ADAMDataSource).

Implicit partition pruning for BAM data source
************************************************

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



