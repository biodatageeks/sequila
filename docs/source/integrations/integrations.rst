Integrations
===============

Integration with R
####################

SeQuiLa comes also with a first-class integration with R environment using sparklyr-sequila R package.

Installation
************

.. code-block:: R

    install.packages('devtools')
    devtools::install_github('ZSI-Bio/bdg-sparklyr-sequila')

Usage
*****

Local mode

.. code-block:: R

    library(sequila)
    library(dplyr)

    #create a connection to SeQuiLa using Spark local mode and 1 thread
    master <- "local[1]"
    driver_mem <- "2g"
    ss<-sequila_connect(master,driver_memory=driver_mem)



YARN

.. code-block:: R

    library(sequila)
    library(dplyr)
    #create a connection to SeQuiLa using Spark yarn-mode with 2 executors
    driver_mem <- "2g"
    executor_mem <- "2g"
    executor_num <- "2"
    master <- "yarn-client"
    ss<-sequila_connect(master,driver_memory<-driver_mem, executor_memory <- executor_mem, executor_num <- executor_num)


Run a query:

.. code-block:: R

    #provided that gr1 and gr2 are Spark tables
    query <- "SELECT gr1.contigName,gr1.start,gr1.end, gr2.start as start_2,gr2.end as end_2 FROM gr1 JOIN gr2
        ON (gr1.contigName=gr2.contigName AND gr1.end >= CAST(gr2.start AS INTEGER)
      AND gr1.start <= CAST(gr2.end AS INTEGER)) order by start"
    #collect spark results to R dataframe
    res<- collect(sequila_sql(ss,'results',query))
    #release Spark resources and close connection
    sequila_disconnect(ss)

Integration with JDBC
#######################

.. figure:: thrift-server.*
    :align: center


The easiest way to start a SequilaThriftServer it to use our Docker image, e.g.:

.. code-block:: bash

    docker run -e USERID=$UID -e GROUPID=$(id -g) \
    -it --rm -p 10000:10000 -p 4040:4040 biodatageeks/|project_name|:|version| \
    bdg-start-thriftserver --master=local[2] --driver-memory=2g

It can be further accessed using any JDBC tool, for example beeline or SquirrelSQL:

Beeline
*******


.. code-block:: bash

    beeline -u jdbc:hive2://cdh00:10000


SquirrelSQL
***********

You will need Spark JDBC driver. We have prepared an assembly jar for this purpose: http://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/org/biodatageeeks/spark/jdbc/spark-jdbc_2.11/0.12/spark-jdbc_2.11-0.12-assembly.jar

Squirrel SQL configure new driver:

.. figure:: jdbc.*
    :align: center

Create new Alias:

.. figure:: alias.*
   :scale: 50%
   :align: center

    Afterwards you can play with SQL.

Running on YARN
################
Running SeQuiLa tools on non-kerberized(not secured) Hadoop cluster is pretty straightforward:

.. code-block:: bash

    #create a home dir for bdgeek user on HDFS
    sudo -u hdfs hadoop fs -mkdir /user/bdgeek
    sudo -u hdfs hadoop fs -chown -R bdgeek:bdgeek   /user/bdgeek

    #run bdg-shell with 2 executors
    docker run --rm --network=host -v /etc/hadoop/conf:/etc/hadoop/conf \
    -e USERID=$UID -e GROUPID=$(id -g) -p 4040:4040 \
    -t biodatageeks/|project_name|:|version| bdg-shell \
    --master yarn-client --num-executors 2 --executor-memory 2g --driver-memory 2g


Once started you should be able to see you bdg-shell app in YARN web UI:

.. figure:: yarn.*
    :align: center

.. note::

    For detailed instructions on how to deploy Apache Spark applications on YARN  please check this `page <https://spark.apache.org/docs/latest/submitting-applications.html>`_.
    Please note that all options including resource management can be set in exactly the same way for all SeQuiLa's apps including: bdg-shell and featureCounts.

