

Quickstart 
==========


SeQuiLa is perfect for quick, ad-hoc analysis. Get ready for quickstart with SeQuiLa.

First we will verify installation of SeQuiLa.

Test run at local machine
#########################

Sequila is distributed as Docker image. It is available at DockerHub. 

The quickest way to test sequila is to run test example on sample data which are already packaged in docker container.

.. note::

   Prerequisities: You should have docker daemon installed and running. `<https://docs.docker.com/install/>`_


In your command line pull docker image from Docker Hub and invoke smoketests

.. code-block:: bash

   docker pull biodatageeks/|project_name|:|version|

   docker run -e USERID=$UID -e GROUPID=$(id -g) \
     biodatageeks/|project_name|:|version| \
     bdg-shell -i /tmp/smoketest.scala



This will open bdg-shell (wrapper for spark-shell) and load ``smoketest.scala`` code for execution. Since all test data are already packaged and accessible within the container you may just wait for results. Which won't take too long. (Estimated time ~ 1 minute)

At the end you should see the following output:

.. image:: quick_start_check.*

From the screenshot above you can see that our optimized IntervalTree-based join strategy was used. Some additional debug information were logged to the console.

The final result should be as follows:
::

   TEST PASSED


Congratulations! Your installation is working on sample data.

.. note::

   If you are wondering what this part ``-e USERID=$UID -e GROUPID=$(id -g)``  is for: It allows the Docker container's inside-user to write in mounted volumes with host's user id and group id.



Launch bdg-shell
****************

Here we will launch bdg-shell which is actually spark-shell wrapped by biodatageeks with some additional configuration.
So if you are familiar with Scala you will be able to use SeQuiLa right away.

.. code-block:: bash


   docker run -e USERID=$UID -e GROUPID=$(id -g) \
   	-it --rm biodatageeks/|project_name|:|version| \
     bdg-shell 

And voila you should see bdg-shell collecting its depenedencies and starting off. Now you are ready to load your sample data and do some interval queries playing on your own.

Launch spark-shell
********************

If for any reason you do not want to use bdg-shell and prefer pure spark-shell you can of course do that. But to use SeQuiLa's efficient interval queries you have to configure it appropriately.

.. code-block:: bash


   docker run -e USERID=$UID -e GROUPID=$(id -g) \
   	-it --rm biodatageeks/|project_name|:|version| \
     spark-shell --packages org.biodatageeks:bdg-sequila_2.11:|version| \
  		--conf spark.sql.warehouse.dir=/home/bdgeek/spark-warehouse \
 		--repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/

And inside the shell:

.. code-block:: scala

   import org.biodatageeks.utils.{SequilaRegister, UDFRegister}

   /*set params*/

   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder","false")
   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (128*1024*1024).toString)

   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")

   /*register UDFs*/

   UDFRegister.register(spark)

   /*inject bdg-granges strategy*/
   SequilaRegister.register(spark)

It seems like there is a lot of configuration required - therefore we recommend using bdg-shell instead.

.. note::

   There are many other ways of how you can use SeQuiLa. Please refer to :doc:`../usage/usage`


Ad-hoc analysis
#################

For ad-hoc analysis SeQuiLa provides two usage patterns:

Using predefined scripts in docker container
**********************************************

In SeQuiLa's docker image are two predefined scripts written to be executable from commandline in good-old fashion.  No need of Scala and Spark is needed.

   |

.. figure:: docker.*

   
Sample usage of SeQuiLa wrapped in docker container's scripts.
The snippet below shows how to download sample data files into specific directory, then run the container with mounted volume.
The result should appear in specified output directory.


.. code-block:: bash

   cd  /data/sequila

   wget http://biodatageeks.org/sequila/data/NA12878.slice.bam

   wget http://biodatageeks.org/sequila/data/tgp_exome_hg18.saf

   docker run --rm -it -p 4040:4040 \ 
      -v /data/sequila:/data \ 
      -e USERID=$UID -e GROUPID=$(id -g) \
      biodatageeks/|project_name|:|version| \
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



