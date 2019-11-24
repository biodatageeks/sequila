

Quickstart 
===========


Use SeQuiLa Docker image
#########################

SeQuiLa is perfect for quick, ad-hoc analysis. Sequila is distributed as Docker image. It is available at DockerHub.

In SeQuiLa's docker image are two predefined scripts written to be executable from commandline in good-old fashion.  No need of Scala and Spark is needed.

   |

.. figure:: docker.*


Test run
**********


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

The final result should be as follows: ``TEST PASSED``

Congratulations! Your installation is working on sample data.

.. note::

   If you are wondering what this part ``-e USERID=$UID -e GROUPID=$(id -g)``  is for: It allows the Docker container's inside-user to write in mounted volumes with host's user id and group id.



featureCounts script
*********************

Sample usage of SeQuiLa wrapped in docker container's scripts.
The snippet below shows how to download sample data files into specific directory, then run the container with mounted volume.
The result should appear in specified output directory.


Parameters passed to featureCounts are divided into two parts: equivalent to parameters passed for spark-submit (master, executor-memory, driver-memory etc.: `<https://spark.apache.org/docs/latest/submitting-applications.html>`_) and parameters passed to featureCounts itself (input files, output files, format).


.. code-block:: bash

   mkdir -p /data/sequila
   
   cd  /data/sequila

   wget http://biodatageeks.org/sequila/data/NA12878.slice.bam

   wget http://biodatageeks.org/sequila/data/tgp_exome_hg18.saf

   docker run --rm -it -p 4040:4040 \ 
      -v /data/sequila:/data \ 
      -e USERID=$UID -e GROUPID=$(id -g) \
      biodatageeks/|project_name|:|version| \
      featureCounts -- \ 
      -o /data/featureOutput.bed -F SAF \
      -a /data/tgp_exome_hg18.saf /data/NA12878.slice.bam

   head /data/featureOutput.bed






.. note::

   If you are using zsh shell remember to put double-quotes (") when specifying master local with specified number threads. ``--master "local[4]"``


depthOfCoverage script
***********************

Parameters passed to depthOfCoverage are divided into two parts: equivalent to parameters passed for spark-submit (master, executor-memory, driver-memory etc.: `<https://spark.apache.org/docs/latest/submitting-applications.html>`_) and parameters passed to featureCounts itself (input reads file, output file, format).


.. code-block:: bash

   mkdir -p /data/sequila
   
   cd  /data/sequila

   wget http://biodatageeks.org/sequila/data/NA12878.slice.bam

   docker run --rm -it  \ 
      -v /data/sequila:/data \ 
      -e USERID=$UID -e GROUPID=$(id -g) \
      biodatageeks/|project_name|:|version| \
      depthOfCoverage --master local --driver-memory=2g -- \ 
      -r /data/NA12878.slice.bam -o /data/NA12878.cov.bed \
      -f blocks 
      

      head /data/NA12878.cov.bed

       contigName      start   end     coverage
       chr1    34      34      1
       chr1    35      35      2
       chr1    36      37      3
       chr1    38      40      4
       chr1    41      49      5
       chr1    50      67      6
       chr1    68      109     7
       chr1    110     110     6
       chr1    111     111     5




.. note::

   If you are using zsh shell remember to put double-quotes (") when specifying master local with specified number threads. ``--master "local[4]"``


bdg-shell in container
**********************

Here we will launch bdg-shell which is actually spark-shell wrapped with some additional configuration.
So if you are familiar with Scala you will be able to use SeQuiLa right away.

.. code-block:: bash


   docker run -e USERID=$UID -e GROUPID=$(id -g) \
      -it --rm biodatageeks/|project_name|:|version| \
     bdg-shell 

And voila you should see bdg-shell collecting its depenedencies and starting off. Now you are ready to load your sample data and do some interval queries or coverage analyses on your own.


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



spark-shell in container
*************************

If for any reason you do not want to use bdg-shell and prefer pure spark-shell you can of course do that. But to use SeQuiLa's efficient interval queries or depth of coverage modules you have to configure it appropriately.

.. code-block:: bash


   docker run -e USERID=$UID -e GROUPID=$(id -g) \
      -it --rm biodatageeks/|project_name|:|version| \
     spark-shell --packages org.biodatageeks:bdg-sequila_2.11:|version| \
      --conf spark.sql.warehouse.dir=/home/bdgeek/spark-warehouse \
      --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/

And inside the shell:

.. code-block:: scala

   import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister}

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

Afterwards you can proceed with e.g. depth of coverage calculations

.. code-block:: scala

   val tableNameBAM = "reads"
  //
  // path to your BAM file 
  val bamPath = "file:///Users/aga/workplace/data/NA12878.chr21.bam"
  // create database DNA
  ss.sql("CREATE DATABASE dna")
  ss.sql("USE dna")

   // create table reads using BAM data source
   ss.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
    """.stripMargin)

  //calculate coverage - example for blocks coverage
  
  ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks')").show(5)
  
          +----------+-----+---+--------+
          |contigName|start|end|coverage|
          +----------+-----+---+--------+
          |      chr1|   34| 34|       1|
          |      chr1|   35| 35|       2|
          |      chr1|   36| 37|       3|
          |      chr1|   38| 40|       4|
          |      chr1|   41| 49|       5|
          +----------+-----+---+--------+


Use SeQuiLa directly
######################

SeQuiLa can be used directly as an extension to Apache Spark. We are publishing SeQuiLa JAR files in public repositories: https://zsibio.ii.pw.edu.pl/nexus/#browse/browse/components:maven-snapshots and https://zsibio.ii.pw.edu.pl/nexus/#browse/browse/components:maven-releases. 


Analyses in spark-shell
*************************

.. note::

   Prerequisities: For execution in local mode, you should have installed Apache Spark on your machine. When executing on computation cluster you should have setup Spark ecosystem up and running (including HDFS, YARN and Spark itself)

In order to use our extensions when performing analysis in spark-shell you need to pass SeQuiLa library dependency to Spark.

.. code-block:: bash

  cd $SPARK_HOME/bin

  # 1
  # run spark shell with SeQuiLa passed as dependency
  # include additional repositories to download JAR file
  # run in local mode, specified required driver memory

  ./spark-shell -v \
  --master=local[10]
  --driver-memory=12g  \
  --repositories http://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,http://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ \
  --packages org.biodatageeks:bdg-sequila_2.11:|version|

  # 2
  # run spark shell with SeQuiLa passed as dependency
  # include additional repositories to download JAR file
  # run on cluster using YARN, specified number of executors and required memory for executors 

    ./spark-shell -v \
  --master=yarn --deploy-mode=client \
  --num-executors=60 --executor-memory=4g \
  --driver-memory=12g  \
  --repositories http://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,http://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ \
  --packages org.biodatageeks:bdg-sequila_2.11:|version|


  # 3
  # download assembly file and store in /tmp

  wget -P /tmp \
   org/biodatageeks/bdg-sequila_2.11/|version|/bdg-sequila_2.11-|version|-assembly.jar

  # run spark shell with SeQuiLa passed as assembly JAR
  # run in local mode, specified required driver memory

    ./spark-shell -v \
  --master=local[10]
  --driver-memory=12g  \
  --jars /tmp/bdg-sequila_2.11-|version|-assembly.jar


Once the spark-shell with SeQuiLa extension has been startup you can  import necessary classes and perform tha analyses.




.. code-block:: scala

   import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister}

   /*set params*/

   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder","false")
   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (128*1024*1024).toString)

   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
   spark.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")

   /*register UDFs*/

   UDFRegister.register(spark)

   /*inject bdg-granges strategy*/
   SequilaRegister.register(spark)
   val tableNameBAM = "reads"
  //
  // path to your BAM file 
  val bamPath = "file:///Users/aga/workplace/data/NA12878.chr21.bam"
  // create database DNA
  ss.sql("CREATE DATABASE dna")
  ss.sql("USE dna")

   // create table reads using BAM data source
   ss.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
    """.stripMargin)

  //calculate coverage - example for blocks coverage
  
  ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks')").show(5)
  
          +----------+-----+---+--------+
          |contigName|start|end|coverage|
          +----------+-----+---+--------+
          |      chr1|   34| 34|       1|
          |      chr1|   35| 35|       2|
          |      chr1|   36| 37|       3|
          |      chr1|   38| 40|       4|
          |      chr1|   41| 49|       5|
          +----------+-----+---+--------+


Use SeQuiLa directly in Scala app
*************************************

If you want to embedd SeQuiLa in your Scala application and run it on Spark cluster you need to add SeQuiLa as dependency in build.sbt file and include biodatageeks repositories as additional resolvers.

build.sbt

.. code-block:: scala

    libraryDependencies +=  "org.biodatageeks" % "|project_name|_2.11" % "|version|"

    resolvers +=  "biodatageeks-releases" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/"
    resolvers +=  "biodatageeks-snapshots" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/"

In your code you need to import required classes, create and register SequilaSession and you are ready to go.

.. code-block:: scala

    import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister}
    import org.apache.spark.sql.SequilaSession
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
        .getOrCreate()
    val ss = new SequilaSession(spark)
    SequilaRegister.register(ss)
    UDFRegister.register(ss)
    ss.sql(...)

Finally, once the whole application is ready you can submit it to Spark cluster using spark-submit command accordingly to `Spark documentation <https://spark.apache.org/docs/latest/submitting-applications.html>`_ 

.. code-block:: bash
  
  spark-submit --master=yarn  --deploy-mode=client /path/to/jar/bioinfoAnalysis.jar


