 .. sectnum::
     :start: 4

SeQuiLa API
===========


The API that is provided by SeQuiLa is not a set of predifined functions but rather exposing flexible and enhanced SQL interface to quering genomic datasets. Enhanced in terms of speed in performing range joins between datasets which is crucial to data analysis in bioinformatics

Below we will describe how to construct SQL queries to efficiently join genomic datasets with SeQuiLa.




General usage
##############
Let's assume that you already have prepared or imported two datasets ``s1`` and ``s2`` which both contain location indices of start and end of interval (usually named: start, end). Optionally you may also want to take chromosome name into account (usually chr). 

You have to registral SeQuiLa extrastrategy and prepare SQL query that will be performed.

Find overlaps - basic
***********************

Range join on interval on two datasets can be defined in SQL. You may want to select all resulting columns ```select * ``` or just a subset of your preference.  As a result of performing this query by SparkSQL you will get results stored in another dataset

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

If your dataset contains chromosomes information as well you may want to add additional constraint to join condition so as to find only overlaps from the same chromosome.

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

SeQuiLa introduces several UserDefinedFunctions. 

shift
******

Shift function is performing operation of shifting the ranges by a specified number of positions.
To use the function within query it needs to be registered. Sample query using shift function:

::

   spark.sqlContext.udf.register("shift", RangeMethods.shift _)

   val query =
     s"""
      |SELECT chr,start,end,
      |shift(start,5) as start_2 ,
      |shift(end,5) as end_2 
      |FROM ref LIMIT 1
    """.stripMargin


resize
*******

Resize function is performing operation of extending the range by specified width. Sample query using resize function:

::

   spark.sqlContext.udf.register("resize", RangeMethods.resize _)

   val query =
      s"""
        |SELECT chr,start,end,
        |resize(start,end,5,"center")._1 as start_2,
        |resize(start,end,5,"center")._2 as end_2 
        |FROM ref LIMIT 1
   """.stripMargin

calcOverlap
************

calcOverlap function returns the width of overlap between intervals. To use the function within query it needs to be registered. Sample query using overlaplenght function:

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



Additional parameteres
######################

Currently SeQuiLa provides three additional parameters that impact joining in terms of results and speed of execution


minOverlap
***********
This parameter is defining the minimal overlapping positions for interval. The default value is set to 1, meaning that two intervals are considered overlapping if they have at least one position in common.

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
This parameter is defining the decision boundary for choosing to broadcast whole table (with all columns) to the tree (prefered for narrow dataframes) or just intervals (preferred for wider dataframes). When whole table is broadcast the solution os more memory-demanding but joining happens in one step. When just intervals are broadcast joining happens in two steps.

By default the parameter is set to 10240 kB

Parameter is set via coniguration:
::

   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (10*(1024*1024)).toString)


