Interval joins
===============

.. contents::

Operations
############

find overlaps - perform range join query
-------------------------------------------

The operation of joining two data sets is expressed in SQL as standard non-equi join where join conditions are expressed as overlapping coordinates.

.. code-block:: scala

	// query where chromosome information is not included in the join condition
   val query = 
      s"""
        |SELECT * 
        |FROM s1 JOIN s2 
        |ON (end1>=start1 and start1<=end2 )
        """
   val res = ss.sql(query)

	// query where chromosome information is included in the join condition
   val queryChrom = 
        s"""
        |SELECT * 
        |FROM s1 JOIN s2 
        |ON (s1.chr=s2.chr and s1.end>=s2.start and s1.start<=s2.end )
        """
     val resChrom = spark.sql(queryChrom)


This result can be then used for various data manipulations and for further pipeline implementation.


bdg_shift - shift interval ranges
------------------------------------

Shift function performs an operation of shifting ranges by a specified number of positions. A sample query using the shift function:

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

bdg_resize - manipulate the range of interval
-----------------------------------------------

Resize function performs an operation of extending the range by a specified width.
It returns range with start and end fields. A sample query using the resize function:

::

    val query =
     s"""
        |SELECT start, end, bdg_resize(start,end,5,"center") as resizedInterval FROM ref
      """.stripMargin

bdg_overlaplength - calculate the overlap length
---------------------------------------------------

bdg_overlaplength function returns the width of overlap between intervals.
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

bdg_flank - calculte the flanking range
-----------------------------------------

Flank function performs an operation of calculating the flanking range with specified width. The first boolean argument indicates whether flanking should be performed from start of range (true) or end (false).
The second boolean argument set to true indicates that flanking range should contain not only outside of original range, but also inside.
In that case width of flanking range is doubled. Flank function returns range with start and end fields. A sample query using the flank function:

::

    val query =
      s"""
        |SELECT start, end, bdg_flank(start,end,5,true,true) as flankedInterval FROM ref
       """.stripMargin
   
bdg_promoters
---------------

Promoters function performs an operation of calculating promoter for the range with given upstream and downstream.
It returns range with start and end fields. A sample query using the promoters function:

::

    val query =
      s"""
        |SELECT start, end, bdg_promoters(start,end,100,20) as promoterInterval FROM ref
       """.stripMargin

bdg_reflect - reverse the range
--------------------------------

Reflect function performs and operation of reversing the range relative to specified reference bounds.
It returns range with start and end fields. A sample query using the reflect function:

::

    val query =
      s"""
        |SELECT start, end, bdg_reflect(start,end,11000,15000) as reflectedInterval FROM ref
       """.stripMargin 
   
   


Functional parameteres
######################

ss is a SequilaSession object created as follows:

.. code-block:: scala

    import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
    import org.apache.spark.sql.SequilaSession
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
        .getOrCreate()
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    UDFRegister.register(ss)




minOverlap - determine minimal number of overlapping positions 
-----------------------------------------------------------------
This parameter defines the minimal overlapping positions for interval.
The default value is set to 1, meaning that two intervals are considered as overlapping if they have at least one position in common.

Parameter can be set in the following way:
::
   
   ss.sqlContext.setConf("minOverlap","5")



maxGap - determine maximum distance between intervals
--------------------------------------------------------

This parameter defines possible separation of intervals of maxGap or less and still consider them as overlapping. The default is equal to 0.

Parameter can be set in the following way:
::

   ss.sqlContext.setConf("maxGap","10")



Performance tuning parameters
###############################

maxBroadcastSize - control the size of broadcast variable
----------------------------------------------------------
This parameter defines the threshold for the decision whether to broadcast whole table (with all columns referenced in a query) to the tree (preferred smaller dataframes)
or just intervals (preferred for very large dataframes). If the whole table is broadcasted the solution
is more memory-demanding but joining happens in one step. If just intervals are broadcast joining happens in two steps.

By default the parameter is set to 10240 kB

Parameter can be set in the following way:
::

   ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (10*(1024*1024)).toString)


useJoinOrder - manually decide about the join order
-----------------------------------------------------
If this parameter is set to FALSE the algorithm itself decides which table is used for broadcasting.
It performs row counting on both tables and chooses smaller one.

To achieve even better performance you can set this parameter to TRUE.
In this case, the algorithm does not check table sizes but blindly broadcasts the second table.
You should use this parameter if you know approx. table sizes beforehand.

By default the parameter is set to false.

Parameter can be set in the following way:

.. code-block:: scala

	spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder", "true")

