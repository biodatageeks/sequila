 .. sectnum::
    :start: 5

Usecases
=========

FeatureCounts
#############


Multisample analyses
#####################


.. code-block:: bash

    MacBook-Pro:multisample marek$ ls -ltr
    total 1424
    -rw-r--r--  1 marek  staff  364043 Mar 22 19:32 NA12878.slice.bam
    -rw-r--r--  1 marek  staff  364043 Mar 22 19:32 NA12879.slice.bam
    MacBook-Pro:multisample marek$ pwd
    /Users/marek/git/forks/bdg-spark-granges/src/test/resources/multisample
    MacBook-Pro:multisample marek$


.. code-block:: bash

    docker run -p 4040:4040 -it --rm -e USERID=$UID -e GROUPID=$(id -g) \
    -v /Users/marek/git/forks/bdg-spark-granges/src/test/resources/:/data/input \
    biodatageeks/bdg-toolset bdg-shell


.. code-block:: scala

    val tableNameBAM = "reads"
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil
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

.. code-block:: bash

    +--------+----------+-----+---+-----+
    |sampleId|contigName|start|end|cigar|
    +--------+----------+-----+---+-----+
    | NA12878|      chr1|   34|109|  76M|
    | NA12878|      chr1|   35|110|  76M|
    | NA12878|      null|   36|  0|    *|
    | NA12878|      chr1|   36|111|  76M|
    | NA12878|      chr1|   38|113|  76M|
    +--------+----------+-----+---+-----+

    only showing top 5 rows

.. code-block:: scala

    spark.sql("SELECT distinct sampleId FROM reads").show(5)

.. code-block:: bash

    +--------+
    |sampleId|
    +--------+
    | NA12878|
    | NA12879|
    +--------+
