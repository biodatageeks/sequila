

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

    import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister}
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
   
   
coverage
************

coverage is a function that calculates depth of coverage for specified sample. It can return results in blocks (which is default, more efficient behaviour), with per base granularity or calculate avarage coverage in fixed length window 


.. code-block:: scala

   val tableNameBAM = "reads"
  val bamPath = "file:///Users/aga/workplace/data/NA12878.chr21.bam"
  ss.sql("CREATE DATABASE dna")
  ss.sql("USE dna")

   // CREATE TABLE USING DATASOURCE
   ss.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
    """.stripMargin)

  //CALCULATE COVERAGE - BLOCKS RESULT
  
  ss.sql(s"SELECT * FROM coverage('${tableNameBAM}','NA12878.chr21', 'blocks')").show(5)
  
          +----------+-----+---+--------+
          |contigName|start|end|coverage|
          +----------+-----+---+--------+
          |      chr1|   34| 34|       1|
          |      chr1|   35| 35|       2|
          |      chr1|   36| 37|       3|
          |      chr1|   38| 40|       4|
          |      chr1|   41| 49|       5|
          +----------+-----+---+--------+
  
  
  //CALCULATE COVERAGE - BASES RESULT
  
  ss.sql(s"SELECT contigName, start, coverage FROM coverage('${tableNameBAM}','NA12878.chr21', 'bases')").show(5)
  
          +----------+-----+--------+
          |contigName|start|coverage|
          +----------+-----+--------+
          |      chr1|   34|       1|
          |      chr1|   35|       2|
          |      chr1|   36|       3|
          |      chr1|   37|       3|
          |      chr1|   38|       4|
          +----------+-----+--------+
  
  //CALCULATE COVERAGE - FIXED LENGTH WINDOWS 
  
  ss.sql(s"SELECT * FROM coverage('${tableNameBAM}','NA12878.chr21', 'blocks', 100)").show(5)
          +----------+-----+---+--------+
          |contigName|start|end|coverage|
          +----------+-----+---+--------+
          |      chr1|    0| 99|6.030303|
          |      chr1|  200|299|    1.68|
          |      chr1|  500|599|    4.69|
          |      chr1|  100|199|    1.61|
          |      chr1|  400|499|    3.05|
          |      chr1|  300|399|    1.82|
          +----------+-----+---+--------+

Parameters for coverage functions:
resultType - blocks or bases (blocks by default)
target - size of fixed-length windows
ShowAllPositions - true/false. When set to true returns all positions in contig.


Functional parameteres
######################

ss is a SequilaSession object created as follows:

.. code-block:: scala

    import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister}
    import org.apache.spark.sql.SequilaSession
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
        .getOrCreate()
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    UDFRegister.register(ss)




minOverlap
***********
This parameter defines the minimal overlapping positions for interval.
The default value is set to 1, meaning that two intervals are considered as overlapping if they have at least one position in common.

Parameter can be set in the following way:
::
   
   ss.sqlContext.setConf("minOverlap","5")



maxGap
*******

This parameter defines possible separation of intervals of maxGap or less and still consider them as overlapping. The default is equal to 0.

Parameter can be set in the following way:
::

   ss.sqlContext.setConf("maxGap","10")



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

   ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (10*(1024*1024)).toString)


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





