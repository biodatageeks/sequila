 .. sectnum::
     :start: 3

Usage
=====



Ad-hoc analysis
#################

For ad-hoc analysis SeQuiLa provides two usage patterns:

Using predefined scripts in docker container
**********************************************

In SeQuiLa's docker image are two predefined scripts written to be executable from commandline in good-old fashion.  No need of Scala and Spark is needed.

   |

.. figure:: docker.*

   
Sample usage of SeQuiLa wrapped in docker container's scripts. The snippet below shows how to download sample data files into specific directory, then run the container with mounted volume. The result should appear in specified output directory.


.. code-block:: bash

   cd  /data/sequila

   wget http://.../NA12878.slice.bam

   wget http://.../tgp_exome_hg18.saf

   docker run --rm -it -p 4040:4040 \ 
      -v /data/sequila:/data \ 
      -e USERID=$UID -e GROUPID=$(id -g) \
      biodatageeks/bdg-toolset \ 
      featureCounts -- \ 
      -o /data/featureOutput -F SAF \
      -a /data/tgp_exome_hg18.saf /data/NA12878.slice.bam

Parameters passed to featureCounts are divided into two parts: equivalent to parameters passed for spark-submit (master, executor-memory, driver-memory etc.: `<https://spark.apache.org/docs/latest/submitting-applications.html>`_) and parameters passed to featureCounts itself (input files, output files, format).


.. note::

   If you are using zsh shell remember to put double-quotes (") when specifying master local with specified number threads. ``--master "local[4]"``


Writing short analysis in bdg-shell
************************************

For Scala enthusiasts - SeQuiLa provides bdg-shell which is a wrapper for spark-shell. It has extra strategy registered  and configuration already set, so it is fit for quick analysis.

   |

.. figure:: bdg-shell.*

   Sample ad-hoc analysis


.. code-block:: scala

   import htsjdk.samtools.ValidationStringency
   import org.apache.hadoop.io.LongWritable
   import org.apache.spark.SparkContext
   import org.apache.spark.rdd.NewHadoopRDD
   import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}
   import org.seqdoop.hadoop_bam.util.SAMHeaderReader


   sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.toString)
   case class PosRecord(contigName:String,start:Int,end:Int)

   val alignments = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat]("/data/granges/NA12878.ga2.exome.maq.recal.bam").map(_._2.get).map(r=>PosRecord(r.getContig,r.getStart,r.getEnd))

   val reads=alignments.toDF
   reads.createOrReplaceTempView("reads")

   val targets = spark.read.parquet("/data/granges/tgp_exome_hg18.adam")
   targets.createOrReplaceTempView("targets")

   val query="""    SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
            |         ON (targets.contigName=reads.contigName
            |         AND
            |         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
            |         AND
            |         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
            |         )
            |         GROUP BY targets.contigName,targets.start,targets.end"""

   val reads = spark.read.parquet("/data/granges/NA12878.ga2.exome.maq.recal.adam")
   reads.createOrReplaceTempView("reads")

   val targets = spark.read.parquet("/data/granges/tgp_exome_hg18.adam")
   targets.createOrReplaceTempView("targets")
   sqlContext.sql(query)


------------

Integration with existing applications
#######################################



Integration with Spark-application
***********************************
When you have exisiting analysis pipeline in Spark ecosystem you may beenfit from SeQuiLa extra strategy registered at SparkSQL level.


.. figure:: spark-integration.* 
   :align: center

<TODO> opis krokow



Integration with non Spark-application
***************************************

.. figure:: thrift-server.* 
   :align: center




Start Spark Thrift Server:

.. code-block:: bash

    export BGD_VERSION=0.3-SNAPSHOT
    sudo -u superset HADOOP_CONF_DIR=/etc/hadoop/conf ./start-thriftserver.sh --master yarn \
    --executor-memory 4g --num-executors 10 --executor-cores 4  --driver-memory 4g \
    --hiveconf hive.server2.thrift.port=12000 --conf spark.sql.hive.thriftServer.singleSession=true \
    spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-8-oracle --packages org.biodatageeks:bdg-spark-granges_2.11:${BGD_VERSION} \
    --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/

At your favourite SQL client setup connection to Spark Thrift Server.

You will need Spark JDBC driver. We have prepared assembly jar for this purpose: http://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/org/biodatageeeks/spark/jdbc/spark-jdbc_2.11/0.12/spark-jdbc_2.11-0.12-assembly.jar

For example in Squirrel SQL configure new driver:

.. figure:: jdbc.* 
   :align: center

Create new Alias:

.. figure:: alias.* 
   :scale: 50%
   :align: center


Afterwards you can play with SQL.

.. code-block:: sql

    ---reads
    CREATE TABLE granges.NA12878_marek
    USING org.biodatageeks.datasources.BAM.BAMDataSource
    OPTIONS(path "/data/granges/NA12878.ga2.exome.maq.recal.bam");

    --targets
    CREATE TABLE granges.targets
    USING csv
    OPTIONS (path "/data/granges/tgp_exome_hg18.saf", header "true", inferSchema "false", delimiter "\t");

    SELECT count(*) from granges.NA12878_marek;
    SELECT count(*) from granges.targets limit 1;


    SELECT targets.GeneId AS GeneId,
                         targets.Chr AS Chr,
                         targets.Start AS Start,
                         targets.End AS End,
                         targets.Strand AS Strand,
                         CAST(targets.End AS INTEGER)-CAST(targets.Start AS INTEGER) + 1 AS Length,
                         count(*) AS Counts
                FROM granges.NA12878_marek reads JOIN granges.targets targets
    ON (
      targets.Chr=reads.contigName
      AND
      reads.end >= CAST(targets.Start AS INTEGER)
      AND
      reads.start <= CAST(targets.End AS INTEGER)
    )
    GROUP BY targets.GeneId,targets.Chr,targets.Start,targets.End,targets.Strand;

Integration with R
##################

1. Install rJava and RJDBC packages:

.. code-block:: R

    install.packages("RJDBC",dep=TRUE)
    install.packages("rJava")
    library(RJDBC)

2. Download Spark JDBC driver - for the convenience we have already prepare a self-contained jar file for you:

.. code-block:: R

    download.file("http://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/org/biodatageeeks/spark/jdbc/spark-jdbc_2.11/0.12/spark-jdbc_2.11-0.12-assembly.jar",destfile = "spark-jdbc-assembly-0.12.jar")

3. Establish a connection to the Spark Thrift Server you have started in the previous section:

.. code-block:: R

    drv <- JDBC("org.apache.hive.jdbc.HiveDriver",classPath = "./spark-jdbc-assembly-0.12.jar",identifier.quote="`")
    conn <- dbConnect(drv, "jdbc:hive2://cdh00:12000", "user", "passord")


    ds <-dbGetQuery(conn, "SELECT targets.GeneId AS GeneId,
                         targets.Chr AS Chr,
                    targets.Start AS Start,
                    targets.End AS End,
                    targets.Strand AS Strand,
                    CAST(targets.End AS INTEGER)-CAST(targets.Start AS INTEGER) + 1 AS Length,
                    count(*) AS Counts
                    FROM granges.NA12878_marek reads JOIN granges.targets targets
                    ON (
                    targets.Chr=reads.contigName
                    AND
                    reads.end >= CAST(targets.Start AS INTEGER)
                    AND
                    reads.start <= CAST(targets.End AS INTEGER)
                    )
                    GROUP BY targets.GeneId,targets.Chr,targets.Start,targets.End,targets.Strand")

    nrow(ds)
    head(ds)
    dbDisconnect(conn)

Once done you should be able to see a similar result on your screen:

.. image:: rstudio.*