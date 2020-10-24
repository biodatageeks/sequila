
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
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
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
         |USING org.biodatageeks.sequila.datasources.ADAM.ADAMDataSource
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


Depth of coverage
#################

Performance tests description
*****************************

The main goal of our tests was to compare SeQuiLa-cov performance and scalability with other state-of-the art coverage solutions (samtools ``depth``, bedtools ``genomecov``, GATK ``DepthOfCoverage``, sambamba ``depth`` and mosdepth). The tests were performed on the aligned WES and WGS reads from the NA12878 sample and aimed at calculating blocks and window coverage whenever this functionality was available. Additionally, we performed quality check, veryfing that results generated by SeQuiLa-cov are identical to those returned by samtools ``depth`` and we evaluated the impact of Intel GKL on overall performance.

Test environment setup
----------------------

Infrastructure
--------------

Our tests have been run on a 24-node Hadoop cluster:

======  =============== =========   ===
Role    Number of nodes CPU cores   RAM
======  =============== =========   ===
EN              1         28/56     512
NN/RM           3         28/56     512
DN/NM           24        28/56     512
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

=============   ======= ========================
Software        Version Notes
=============   ======= ========================
HDP             3.0.1
Apache Hadoop   3.1.1
Apache Spark    2.3.1
Oracle JDK      1.8
Scala           2.12
samtools        1.9
bedtools        2.27
GATK            3.8
sambamba        0.6.8
mosdepth        0.2.3
mosdepth        0.2.4   using --fast-mode option
=============   ======= ========================


Datasets for coverage tests
****************************

Two NGS datasets have been used in all the tests.
WES (whole exome sequencing) and WGS (whole genome sequencing) datasets have been used for vertical and horizontal scalability
evaluation respectively. Both of them came from sequencing of NA12878 sample that is widely used in many benchmarks.

In order to remove malformed reads (especially to remove CIGAR and Sequence length inconsistencies) we have processed original BAM files with GATK's tool PrintReads.

.. code-block:: bash

  # also set compression to BAM default level (5) -Dsamjdk.compression_level=5
  gatk --java-options "-Dsamjdk.compression_level=5" PrintReads -I /data/NA12878.hiseq.wgs.bwa.recal.bam -O /data/proper.NA12878.bam


The table below presents basic datasets information:

=========   ======  =========    ========== ========================================================================================== =====================
Data        Format  Size [GB]    Row count   test data URL                                                                              original source 
=========   ======  =========    ========== ========================================================================================== =====================
WES          BAM     17          161544693  `WES BAM <http://biodatageeks.org/sequila/data/WES/NA12878.proper.wes.bam>`_                `original WES BAM <ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20101201_cg_NA12878/NA12878.ga2.exome.maq.recal.bam>`_
WGS          BAM     273         2617420313 `WGS BAM <http://biodatageeks.org/sequila/data/WGS/NA12878.proper.wgs.bam>`_                `original WGS BAM <ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20101201_cg_NA12878/NA12878.hiseq.wgs.bwa.recal.bam>`_
WGS-L        BAM     127         22314075   `WGS-L BAM <http://biodatageeks.org/sequila/data/rel5-guppy-0.3.0-chunk10k.sorted.bam>`_   `original WGS-L BAM <https://s3.amazonaws.com/nanopore-human-wgs/rel5-guppy-0.3.0-chunk10k.sorted.bam>`_
=========   ======  =========    ========== ========================================================================================== =====================


WES - tests performed on a single node using WES dataset

WGS - tests performed on a cluster using WGS dataset

WGS-L - tests performed on a single node using WGS dataset (long reads)


Test procedure
**************
To achieve reliable results and remove test cases have been run 3 times.
.. Before each run disk caches on all nodes have been purged.

File-dataframe mapping
----------------------

The first step of the testing procedure was to prepare mapping between input datasets in BAM format and
its dataframe/table abstraction through our custom data source.


BAM

.. code-block:: scala

    val bamPath = "NA12878*.bam"
    spark.sql(
      s"""
         |CREATE TABLE reads
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)
    spark.sql(s"SELECT contigName,start,end FROM reads LIMIT 1").show()

    +----------+-----+---+
    |contigName|start|end|
    +----------+-----+---+
    |      chr1|   34|109|
    +----------+-----+---+




Coverage calculations
-------------------------

For calculating the coverage the following commands have been used:

.. code-block:: bash

    ### SAMTOOLS 1.9
    #exome - bases 1 core
    { time samtools depth NA12878.proper.wes.bam > samtools/NA12878.proper.wes.bamdepth ; } 2>> samtools/wes_time.txt
    # genome - bases 1 core
    { time samtools depth NA12878.proper.wgs.bam > samtools/NA12878.proper.wgs.bam.depth ; } 2>> samtools/wgs_time.txt

.. code-block:: bash

    ### BEDTOOLS
    # exome blocks 1 core
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 biocontainers/bedtools:v2.27.0_cv2 bedtools genomecov -ibam /data/samples/NA12878/WES/NA12878.proper.wes.bam -bga > /data/samples/NA12878/bedtools_genomecov_block_coverage_wes.txt
    # genome blocks 1 core
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 biocontainers/bedtools:v2.27.0_cv2 bedtools genomecov -ibam /data/samples/NA12878/WGS/NA12878.proper.wgs.bam -bga > /data/samples/NA12878/bedtools_genomecov_block_coverage_wgs.txt

.. code-block:: bash

    ### GATK
    #  exome 1,5,10 cores
    { time docker run -it  -v /data/samples/NA12878/WES:/data/ -v /data/samples/hg_builds/:/ref/ broadinstitute/gatk3:3.8-1  java -jar GenomeAnalysisTK.jar -T DepthOfCoverage -R /ref/Homo_sapiens_assembly18.fasta -o /data/gatk_doc_test.txt -I /data/NA12878.proper.wes.bam  -omitIntervals -nt 1} 2>> gatk_wes_time_1.txt
    { time docker run -it  -v /data/samples/NA12878/WES:/data/ -v /data/samples/hg_builds/:/ref/ broadinstitute/gatk3:3.8-1  java -jar GenomeAnalysisTK.jar -T DepthOfCoverage -R /ref/Homo_sapiens_assembly18.fasta -o /data/gatk_doc_test.txt -I /data/NA12878.proper.wes.bam  -omitIntervals -nt 5} 2>> gatk_wes_time_5.txt
    { time docker run -it  -v /data/samples/NA12878/WES:/data/ -v /data/samples/hg_builds/:/ref/ broadinstitute/gatk3:3.8-1  java -jar GenomeAnalysisTK.jar -T DepthOfCoverage -R /ref/Homo_sapiens_assembly18.fasta -o /data/gatk_doc_test.txt -I /data/NA12878.proper.wes.bam  -omitIntervals -nt 10} 2>> gatk_wes_time_10.txt
    # genome 1 core
    { time docker run -it  -v /data/samples/NA12878/WGS:/data/ -v /data/samples/hg_builds/:/ref/ broadinstitute/gatk3:3.8-1 java -jar GenomeAnalysisTK.jar -T DepthOfCoverage -R /ref/Homo_sapiens_assembly18.fasta -o /data/gatk_doc_test.txt -I /data/NA12878.proper.wgs.bam -omitIntervals -nt 1} 2>> gatk_wgs_time_1.txt
    { time docker run -it  -v /data/samples/NA12878/WGS:/data/ -v /data/samples/hg_builds/:/ref/ broadinstitute/gatk3:3.8-1 java -jar GenomeAnalysisTK.jar -T DepthOfCoverage -R /ref/Homo_sapiens_assembly18.fasta -o /data/gatk_doc_test.txt -I /data/NA12878.proper.wgs.bam -omitIntervals -nt 5} 2>> gatk_wgs_time_5.txt
    { time docker run -it  -v /data/samples/NA12878/WGS:/data/ -v /data/samples/hg_builds/:/ref/ broadinstitute/gatk3:3.8-1 java -jar GenomeAnalysisTK.jar -T DepthOfCoverage -R /ref/Homo_sapiens_assembly18.fasta -o /data/gatk_doc_test.txt -I /data/NA12878.proper.wgs.bam -omitIntervals -nt 10} 2>> gatk_wgs_time_10.txt

.. code-block:: bash

    ### SAMBAMBA
    # exome - blocks 1,5,10 cores
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=1 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=5 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=10 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    
    # exome - windows 1,5,10 cores
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=1 --window-size=500 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=5 --window-size=500 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=10 --window-size=500 /data/samples/NA12878/WES/NA12878.proper.wes.bam

    # genome - blocks 1,5,10 cores
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=1 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=5 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=10 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam

    # genome - windows
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=1 --window-size=500 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=5 --window-size=500 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=10 --window-size=500 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam

.. code-block:: bash

    ### MOSDEPTH v 0.2.3
    # exome blocks 1,5,10 cores
    { time mos/mosdepth prefix NA12878.proper.wes.bam ; } 2>> mos_wes_time_1.txt
    { time mos/mosdepth -t 4 prefix NA12878.proper.wes.bam ; } 2>> mos_wes_time_5.txt
    { time mos/mosdepth -t 9 prefix NA12878.proper.wes.bam ; } 2>> mos_wes_time_10.txt

    # genome blocks 1,5,10 cores
    { time mos/mosdepth prefix NA12878.proper.wgs.bam ; } 2>> wgs_time_1.txt
    { time mos/mosdepth -t 4 prefix NA12878.proper.wgs.bam ; } 2>> wgs_time_5.txt
    { time mos/mosdepth -t 9 prefix NA12878.proper.wgs.bam ; } 2>> wgs_time_9.txt

.. code-block:: bash

    ### MOSDEPTH v 0.2.4 fast
    # exome blocks 1,5,10 cores
    { time mos/mosdepth --fast-mode prefix NA12878.proper.wes.bam ; } 2>> mos_wes_time_1.txt
    { time mos/mosdepth --fast-mode -t 4 prefix NA12878.proper.wes.bam ; } 2>> mos_wes_time_5.txt
    { time mos/mosdepth --fast-mode -t 9 prefix NA12878.proper.wes.bam ; } 2>> mos_wes_time_10.txt

    # genome blocks 1,5,10 cores
    { time mos/mosdepth --fast-mode prefix NA12878.proper.wgs.bam ; } 2>> wgs_time_1.txt
    { time mos/mosdepth --fast-mode -t 4 prefix NA12878.proper.wgs.bam ; } 2>> wgs_time_5.txt
    { time mos/mosdepth --fast-mode -t 9 prefix NA12878.proper.wgs.bam ; } 2>> wgs_time_9.txt

.. code-block:: bash

    ### SEQUILA-COV
    # spark shell started with 1,5,10 cores
    spark-shell  --conf "spark.sql.catalogImplementation=in-memory" --conf spark.dynamicAllocation.enabled=false  --master=yarn-client --driver-memory=4g --executor-memory=4g --num-executors=1 --packages org.biodatageeks:bdg-sequila_2.11:0.4.1-SNAPSHOT --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ -v
    spark-shell  --conf "spark.sql.catalogImplementation=in-memory" --conf spark.dynamicAllocation.enabled=false  --master=yarn-client --driver-memory=4g --executor-memory=4g --num-executors=5 --packages org.biodatageeks:bdg-sequila_2.11:0.4.1-SNAPSHOT --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ -v  
    spark-shell  --conf "spark.sql.catalogImplementation=in-memory" --conf spark.dynamicAllocation.enabled=false  --master=yarn-client --driver-memory=4g --executor-memory=4g --num-executors=10 --packages org.biodatageeks:bdg-sequila_2.11:0.4.1-SNAPSHOT --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ -v  

.. code-block:: scala
    
    // inside spark-shell for SeQuiLa-cov
    import org.apache.spark.sql.SequilaSession
    import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister,BDGInternalParams}
    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, "134217728")
        val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","false")

    /* WES -bases-blocks*/
    ss.sql("""
    CREATE TABLE IF NOT EXISTS reads_exome USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource OPTIONS(path '/data/samples/NA12878/WES/NA12878*.bam')""")
    spark.time{
    ss.sql(s"SELECT * FROM coverage('reads_exome','NA12878', 'blocks')").write.format("parquet").save("/data/samples/NA12878/output_tmp/wes_1_9.parquet")}

    /* WGS -bases-blocks*/
    import org.apache.spark.sql.SequilaSession
    import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister}
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","false")
    /*bases-blocks*/
    ss.sql("""
    CREATE TABLE IF NOT EXISTS reads_genome USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource OPTIONS(path '/data/samples/NA12878/NA12878*.bam')""")
    spark.time{
    ss.sql(s"SELECT * FROM coverage('reads_genome','NA12878', 'blocks')").write.format("parquet").save("/data/samples/NA12878/output_tmp/wgs_1_1.parquet")}

    /*windows - 500*/
    import org.apache.spark.sql.SequilaSession
    import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister}
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","false")
    /*bases-blocks*/
  ss.sql("""
    CREATE TABLE IF NOT EXISTS reads_exome USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource OPTIONS(path '/tmp/data/exome/*.bam')""")
    spark.time{
    ss.sql(s"SELECT * FROM coverage('reads_exome','NA12878', 'blocks', '500')").write.format("parquet").save("/tmp/data/32MB_w500_3.parquet") }

    /* long reads */
    ss.sql("""
      CREATE TABLE IF NOT EXISTS qreads
      USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
      OPTIONS(path '/data/granges/nanopore/guppy.bam')""")

    ss.sql(s"SELECT contigName, start, end, coverage FROM coverage('qreads','NA12878', 'blocks')").write.mode("overwrite").option("delimiter", "\t").csv("/data/granges/nanopore/guppy_cov.bed")}




Apache Spark settings
---------------------

=============== ======
Parameter       Values
=============== ======
driver-memory    8g
executor-memory  4g
executor-cores   1
num-executors    1-500
=============== ======


Results
*******

Detailed results are shown in the table below:


+--------+------------------+---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|data    | operation type   | cores   | samtools   | bedtools   |     GATK     | sambamba   | mosdepth     |  mosdepth fast  |  SeQuiLa-cov   |
+========+==================+=========+============+============+==============+============+==============+=================+================+ 
| WGS    | blocks           | 1       | 2h 14m 58s |10h 41m 27s |    128w *    | 2h 44m 0s  | 1h 46m 27s   | **1h 38m 06s**  |   1h 47m 05s   |
+        +                  +---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|        |                  | 5       |            |            |2d 23h 18m *  | 2h 47m 53s |  36m 13s     |     33m 50s     |   **26m 59s**  |
+        +                  +---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|        |                  | 10      |            |            |2d 17h 6m *   | 2h 50m 47s |  34m 34s     |     33m 16s     |  **13m 54s**   |
+        +------------------+---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|        | windows          | 1       |            |            |              | 1h 46m 50s |**1h 22m 49s**|                 | 1h 24m 08s     |
+        +                  +---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|        |                  | 5       |            |            |              | 1h 41m 23s |  20m 3s      |                 | **18m 43s**    |
+        +                  +---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|        |                  | 10      |            |            |              | 1h 50m 35s |  17m 49s     |                 |   **9m 14s**   |
+--------+------------------+---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
| WES    | blocks           | 1       | 12m 26s    |  23m 25s   |   1d 5h 6m   | 25m 42s    |  6m 43s      |     **6m 12s**  |    6m 54s      |   
+        +                  +---------+------------+------------+--------------+------------+--------------+-----------------+----------------+
|        |                  | 5       |            |            |   3d 0h 24m  | 25m 46s    |  2m 25s      |      2m 21s     |    **1m 47s**  |
+        +                  +---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|        |                  | 10      |            |            |  2d 22h 30m  | 25m 49s    |  2m 20s      |      2m 06s     |    **1m 04s**  |
+        +------------------+---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|        | windows          | 1       |            |            |              | 14m 36s    |  **6m 11s**  |                 |    6m 29s      |
+        +                  +---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|        |                  | 5       |            |            |              | 14m 54s    |  2m 08s      |                 |   **1m 42s**   |
+        +                  +---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 
|        |                  | 10      |            |            |              |  14m 40s   |   2m 14s     |                 |   **1m 1s**    |
+--------+------------------+---------+------------+------------+--------------+------------+--------------+-----------------+----------------+ 

(*) estimated time

On the image below you can find performance and scalability comparison of samtools, mosdepth and SeQuiLa-cov.

.. image:: coverage.*

Base level coverage performance comparison for WES dataset with samtools
------------------------------------------------------------------------

The wall-time on a single core is comparable to the Samtools’ solution. We re-confirmed the scalability of SeQuiLa-cov with the base level output and significant time reduction when executed in distributed environment 

=====   ============== =============== ========
cores   sequila(bases) sequila(blocks) samtools
=====   ============== =============== ========
1       17m 13s           6m 54s        12m 26s
5        4m 17s           1m 47s          -
10       2m 21s           1m 04s          -
=====   ============== =============== ========


CRAM versus BAM performance comparison for WES dataset (blocks)
---------------------------------------------------------------

We observed that timings for processing CRAM files are ~2.5 - 4 times higher than for BAM files.
Importantly, the processing times of the coverage calculation algorithm are equal for BAMs and CRAMs, however the reading stage is significantly slower in case of the latter one. Further speedup of processing CRAM files with SeQuiLa will require significant reimplementation of the data access layer which is an important direction of our future work.



=====   ============== ============
cores   sequila(CRAM)  sequila(BAM)
=====   ============== ============
1        26m 27s          6m 54s
5        4m 35s           1m 47s
10       2m 54s           1m 04s
25       1m 44s           0m 28s
50       1m 15s           0m 20s
=====   ============== ============


Performance of saving coverage results as a single BED file
-----------------------------------------------------------

In order to get coverage reults as a single file we need to explicite use ``coalesce`` method to merge records from
all the partitions before writing them to the storage. Such an approach causes performance degradation
as the data cannot be written in distributed fashion. Equally, due to the fact there is only one thread used for writing, the scalability is impaired as well.

.. code-block:: scala

    import org.apache.spark.sql.SequilaSession
    import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister,BDGInternalParams}

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sql("""CREATE TABLE IF NOT EXISTS reads_exome USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource OPTIONS(path '/data/exome/NA12878.*.bam')""")
    spark.time { ss.sql(s"SELECT * FROM coverage('reads_exome','NA12878', 'blocks')")
            .coalesce(1)
            .write.mode("overwrite")
            .option("delimiter", "\t")
            .csv("/data/granges/exome/coverage.bed")}



=====   ========================   ======================
cores   sequila(BED,coalesce(1))   sequila(Parquet,split)
=====   ========================   ======================
1       11m 59s                    6m 54s
5        4m 56s                    1m 47s
10       4m 05s                    1m 04s
=====   ========================   ======================



Long reads support
---------------------

We have tested coverage calculation for long reads in terms of wall-time and quality of the results. We have achieved identical results with samtools when run in 'bases' mode.

=====   ============== ================
cores   samtools       sequila(blocks)
=====   ============== ================
1        95m 8s         83m 3s
5                       17m 25s
10                      9m 10s
=====   ============== ================

Discussion
-----------
Both samtools and bedtools calculate coverage  using only a single thread, however, their results differ significantly, with samtools being around twice as fast. Sambamba positions itself as a multithreaded solution although our tests revealed that its execution time is nearly constant, regardless of the number of CPU cores used, and even twice as slow as samtools. 

Mosdepth achieved speedup against samtools in blocks coverage and against sambamba in windows coverage calculations, however, its scalability reaches limit at 5 CPU cores. 

Finally, SeQuiLa-cov, achieves nearly identical performance as mosdepth for the single core but the execution time decreases substantially for greater number of available computing resources what makes this solution the fastest when run on multiple cores and nodes.

Our results show that when utilizing additional resources (i.e.  more than 10 CPU cores), SeQuiLa-cov is able to reduce the total computation time to 15 seconds for WES and less than one minute for WGS data. Scalability limit is achieved for  200 and ~500 CPU cores in case of WES and WGS data, respectively. 