.. bdg-spark-granges documentation master file, created by
   sphinx-quickstart on Fri Mar  9 22:03:23 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SeQuiLa User Guide 
====================

SeQuiLa is an ANSI-SQL compliant solution for efficient genomic intervals querying and processing built on top of `Apache Spark`_. Range joins and depth of coverage computations are bread and butter for NGS analysis but the high volume of data make them execute very slowly or even failing to compute.


.. _Apache Spark: https://spark.apache.org/ 

* SeQuiLa is fast:

   - genome-wide analyses in less than a minute (for depth of coverage calculations) and several minutes (for range joins)
   - 22x+ speedup over Spark default join operation
   - up to 100x+ speedup for interval queries for BAM datasource using indexes (>= 0.4.1)
   - 100% accuracy in functional tests against GRanges and samtools

* SeQuiLa is elastic:

   - growing catalogue of utility functions and operations including: `featureCounts`, `countOverlaps` and `bdg_coverage`
   - standard SQL DML/DDL like SPJG (select, predicate, join, group by), CTaS (create table as select), IaS (insert table as select) for easy BAM files manipulation
   - exposed parameters for further performance tuning
   - integration with third-party tools through SparkSQL Thrift JDBC driver
   - can be used natively in R with sparklyr-sequila package
   - possibility to use it as command line tool without any exposure to Scala/Spark/Hadoop
   - Docker images available for a quick start
   - can process data stored both on local as well as on distributed file systems (HDFS, S3, Ceph, etc.)


* SeQuiLa is scalable:

   - implemented in Scala in Apache Spark 2.2 environment 
   - can be run on single computer (locally) or Hadoop cluster using YARN


.. .. rubric:: Main components:


.. .. figure:: architecture/components.*
..     :scale: 100

.. rubric:: Availability:


You can find SeQuiLa publicly available in following repositories:


=================   =====================================================================
Repo                 Link
=================   =====================================================================
GitHub               `<https://github.com/ZSI-Bio/|project_name|>`_
Maven(release)       `<https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/>`_
sparklyr-sequila     `<https://github.com/ZSI-Bio/bdg-sparklyr-sequila/>`_
Docker Hub           `<https://hub.docker.com/r/biodatageeks/|project_name|/>`_
=================   =====================================================================

.. rubric:: Using SeQuiLa in your Scala code :


build.sbt

.. code-block:: scala

    libraryDependencies +=  "org.biodatageeks" % "|project_name|_2.11" % "|version|"

    resolvers +=  "biodatageeks-releases" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/"
    resolvers +=  "biodatageeks-snapshots" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/"

Example.scala

.. code-block:: scala

    import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
    import org.apache.spark.sql.SequilaSession
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
        .getOrCreate()
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    UDFRegister.register(ss)
    ss.sql(...)

.. rubric:: Release notes:

0.5

 - new result type (fixed lenght windows) for depth of coverage calculations


0.4.1

 - a new highly-optimized  `mosdepth <https://github.com/brentp/mosdepth>`_ distributed implementation for depth of coverage calculations
 - BAI indexes support for BAM data source
 - CRAM file format support
 - `Intel Genomics Kernel Library (GKL) <https://github.com/Intel-HLS/GKL>`_ support for BAM files reading
 - CTAS (Create Table As Select) and IAS (Insert As Select) for BAM Files
 - experimental `spark-bam <https://github.com/hammerlab/spark-bam>`_ support

0.4

 - completely rewritten R support as a sparklyr extension
 - experimental support for efficient coverage computation for BAMDatasource exposed as a table-valued function (bdg_coverage)
 - sample pruning mechanism for queries accessing only subset of samples from a table (BAMDatasource)
 - a new JDBC interface based on SequilaThriftServer




.. toctree::
   :numbered:
   :maxdepth: 2

   modules/modules
   architecture/architecture
        integrations/integrations
        function/function
   fileformats/fileformats
   quickstart/quickstart
   usecases/usecases
   benchmarking/benchmarking
   citation/citation



