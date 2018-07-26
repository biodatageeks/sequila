
Benchmarking
=============

Interval joins
##############

Performance tests description
*****************************
In order to evaluate our range join strategy we have run a number of tests using both one-node and a Hadoop cluster
installations. In that way we were able to analyze both vertical (by means of adding computing resources such as CPU/RAM on one machine)
as well as horizontal (by means of adding resources on multiple machines) scalability.

The main idea of the test was to compare performance of SeQuiLa with other tools like featureCounts and genAp that can be used
to compute the number of reads intersecting predefined genomic intervals. It is by no means one of the most commonly used operations
in both DNA and RNA-seq data processing, most notably in gene differential expression and copy number variation-calling.
featureCounts performance results have been treated as a baseline. In order to show the difference between the naive approach using the
default range join strategy available in Spark SQL and SeQuiLa interval-tree one, we have included it in the single-node test.


Test environment setup
----------------------

Infrastructure
--------------

Our tests have been run on a 6-node Hadoop cluster:

======  =============== =========   ===
Role    Number of nodes CPU cores   RAM
======  =============== =========   ===
EN              1           24      64
NN/RM           1           24      64
DN/NM           4           24      64
======  =============== =========   ===

EN - edge node where only Application masters and Spark Drivers have been launched in case of cluster tests.
In case of single node tests (Apache Spark local mode), all computations have been performed on the edge node.

NN - HDFS NameNode

RM - YARN ResourceManager

DN - HDFS DataNode

NM - YARN NodeManager


Software
--------

All tests have been run using the following software components:

=============   =======
Software        Version
=============   =======
CDH             5.12
Apache Hadoop   2.6.0
Apache Spark    2.2.1
ADAM            0.22
featureCounts   1.6.0
Oracle JDK      1.8
Scala           2.11.8
=============   =======


Datasets
********
Two NGS datasets have been used in all the tests.
WES (whole exome sequencing) and WGS (whole genome sequencing) datasets have been used for vertical and horizontal scalability
evaluation respectively. Both of them came from sequencing of NA12878 sample that is widely used in many benchmarks.
The table below presents basic datasets information:

=========   ======  =========    ==========
Test name   Format  Size [GB]    Row count
=========   ======  =========    ==========
WES-SN      BAM     17           161544693
WES-SN      ADAM    14           161544693
WES-SN      BED     0.0045       193557
WGS-CL      BAM     273          2617420313
WGS-CL      ADAM    215          2617420313
WGS-CL      BED     0.0016       68191
=========   ======  =========    ==========

WES-SN - tests performed on a single node using WES dataset

WGS-CL - tests performed on a cluster using WGS dataset


Test procedure
**************
To achieve reliable results test cases have been run 3 times.
Before each run disk caches on all nodes have been purged.

File-dataframe mapping
----------------------

The first step of the testing procedure was to prepare mapping between input datasets (in BAM, ADAM and BED formats)  and
their corresponding dataframe/table abstraction. In case of alignment files our custom data sources has been used, for a BED file Spark's builtin dedicated
for CSV data access.


BAM

.. code-block:: scala

    val bamPath = "NA12878*.bam"
    spark.sql(
      s"""
         |CREATE TABLE reads
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)
    spark.sql(s"SELECT contigName,start,end FROM reads LIMIT 1").show()

    +----------+-----+---+
    |contigName|start|end|
    +----------+-----+---+
    |      chr1|   34|109|
    +----------+-----+---+


ADAM

.. code-block:: scala

    val adamPath = "NA12878*.adam"
    spark.sql(
      s"""
         |CREATE TABLE reads
         |USING org.biodatageeks.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "${adamPath}")
         |
      """.stripMargin)
    spark.sql(s"SELECT contigName,start,end FROM reads LIMIT 1").show()

    +----------+-----+---+
    |contigName|start|end|
    +----------+-----+---+
    |      chr1|   34|109|
    +----------+-----+---+

BED

.. code-block:: scala

    val  bedPath="tgp_exome_hg18.bed"
    spark.sql(s"""
        |CREATE TABLE targets(contigName String,start Integer,end Integer)
        |USING csv
        |OPTIONS (path "file:///${bedPath}", delimiter "\t")""".stripMargin)
    spark.sql("SELECT * FROM targets LIMIT 1").show

    +----------+-----+----+
    |contigName|start| end|
    +----------+-----+----+
    |      chr1| 4806|4926|
    +----------+-----+----+





SQL query for counting features
-------------------------------

For counting reads overlapping predefined feature regions the following SQL query has been used:

.. code-block:: sql

    SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
         ON (targets.contigName=reads.contigName
         AND
         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
         AND
         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
         )
         GROUP BY targets.contigName,targets.start,targets.end

Exactly the same query has been used for both single node and cluster tests.


Apache Spark settings
---------------------

=============== ======
Parameter       Values
=============== ======
driver-memory    8g
executor-memory  4-8g
executor-cores   2-4
num-executors    1-15
=============== ======

Results
*******
SeQuiLa when run in parallel outperforms selected competing tools in terms of speed on single node (1.7-22.1x) and cluster (3.2-4.7x).
SeQuiLa strategy involving broadcasting interval forest with all data columns (SeQuiLa_it_all) performs best
in most of the cases (no network shuffling required), whereas broadcasting intervals with identifiers only (SeQuiLa_it_int)
performs comparable to, or better than GenAp.
All algorithms favours columnar (ADAM) to row oriented (BAM) file format due to column pruning and disk I/O operations reduction.


Local mode
----------

.. image:: local.*


Hadoop cluster
--------------

.. image:: cluster.*


Limitations
-----------

SeQuiLa is slower than featureCounts in a single-threaded applications due to less performat Java BAM reader (mainly BGZF decompression) available
in the Java htsjdk library. We will try to investigate and resolve this bottleneck in the next major release.

Discussion
**********
Results showed that SeQuiLa significantly accelerates  genomic interval queries.
We are aware that paradigm of distributed computing is currently not fully embraced by bioinformaticians therefore we have put
an additional effort into preparing SeQuiLa to be easily integrated into existing applications and pipelines.


Depth calculation
#################