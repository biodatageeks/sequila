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

   Sample usage for findOverlaps


Writing short analysis in bdg-shell
************************************

For Scala enthusiasts - SeQuiLa provides bdg-shell which is a wrapper for spark-shell. It has extra strategy registered  and configuration already set, so it is fit for quick analysis.

   |

.. figure:: bdg-shell.*

   Sample ad-hoc analysis



<TODO> example

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


Running on YARN
##################

Spark Thrift Server and JDBC
############################

.. code-block:: bash

    export BGD_VERSION=0.3-SNAPSHOT
    sudo -u superset HADOOP_CONF_DIR=/etc/hadoop/conf ./start-thriftserver.sh --master yarn \
    --executor-memory 4g --num-executors 10 --executor-cores 4  --driver-memory 4g \
    --hiveconf hive.server2.thrift.port=12000 --conf spark.sql.hive.thriftServer.singleSession=true \
    spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-8-oracle --packages org.biodatageeks:bdg-spark-granges_2.11:${BGD_VERSION} \
    --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/

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