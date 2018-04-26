

Usecases
=========


---------------------------------------------------

DNA-seq analysis
##########################################

RNA-seq analysis
##########################################
Analysis of RNA sequencing data to achieve information about differential expression at the gene level using SeQuiLa tools.
DEG analysis is based on packages: edgeR (https://bioconductor.org/packages/release/bioc/html/edgeR.html)
and DESeq2 (https://bioconductor.org/packages/release/bioc/html/DESeq2.html).
Dataset (GSE22260) comes from NCBI - SRA repository and includes RNA-seq data of 20 samples prostate cancer tumors and 10 samples matched normal tissues.

.. figure:: PipelineRNASeqWithSequila.*
   :scale: 40%
   :align: center

--------------------------------------


.. code-block:: bash


      docker pull biodatageeks/|project_name|:|version|
      docker run -p 4041:4040  -e USERID=$UID -e GROUPID=$(id -g) \
      -it  -v /Users/ales/data/sequila:/data/input biodatageeks/|project_name|:|version| bdg-sequilaR

.. code-block:: R

      #register SeQuilaR extensions
      sparkR.callJStatic("org.biodatageeks.R.SequilaR","init",spark)

      #create db
      sql("CREATE DATABASE dbRNAseq")
      sql("USE dbRNAseq")

      #create data source with reads
      sql('CREATE TABLE reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path "/data/input/*.bam")')

      head(sql('select count(distinct sampleId) from reads'))

.. code-block:: bash

      +-------------------------+
      |count(DISTINCT sampleId) |
      +-------------------------+
      |                      30 |
      +-------------------------+

.. code-block:: R

     #GTF with target regions
     sql('CREATE TABLE targets_temp(Chr string, TypeDB string, Feature string, Start integer, End integer,
                                    t1 varchar(1), Strand varchar(1), t2 varchar(1),
                                    Gene_id_temp varchar(30),Gene_id varchar(20))
          USING csv
          OPTIONS (path "/data/input/Homo_sapiens.gtf", header "false", inferSchema "false", delimiter "\t")')

::

     Depends on needs, build the target table with genes or any features based on gtf source.
     This analysis is based on genes, the targets table contains genes coordinates.

.. code-block:: R

     sql('CREATE TABLE targets as
          SELECT Chr, Start, End, Strand, substr(Gene_id_temp, instr(Gene_id_temp,"E"),15) as Gene_id
          FROM targets_temp
          WHERE Feature="gene" ')

.. 	code-block:: bash

    head(sql('select * from targets'))
    +------+-------+---------+-------+-----------------+
    |   Chr|  Start|      End| Strand|         Gene_id |
    +------+-------+---------+-------+----------------+
    |1  17 61874084| 61874182|    -  |ENSG00000202361  |
    |2  17 61942605| 62065282|    -  |ENSG00000108510  |
    |3  17 62003700| 62007518|    -  |ENSG00000279133  |
    |4  17 62005737| 62006016|    -  |ENSG00000242398  |
    |5  17 62036833| 62036945|    +  |ENSG00000200842  |
    |6  17 62122320| 62122421|    +  |ENSG00000207123  |
    +------+-------+---------+-------+-----------------+

::

  If you need different features (exon or transcript), you can build sql query accordingly.

.. code-block:: R

		 sql('CREATE TABLE targets as
		      SELECT Chr, Start, End, Strand, substr(Gene_id_temp, instr(Gene_id_temp,"E"),15) as Gene_id,
		      CASE WHEN instr(Gene_id_temp,"ENSE") > 0
		           THEN substr(Gene_id_temp, instr(Gene_id_temp,"ENSE"),15)
		           ELSE null END as Exon_id
		      FROM targets_temp
		      WHERE Feature="gene" OR Feature="exon" ')

.. code-block:: bash

    head(sql('select * from targets'))
    +-------+----------+----------+-------+----------------+----------------+
    |   Chr |   Start  |     End  |Strand |        Gene_id |         Exon_id|
    +-------+----------+----------+-------+----------------+----------------+
    |1   2  | 101050401| 101050641|      -| ENSG00000204634| ENSE00001710012|
    |2   2  | 101040178| 101040385|      -| ENSG00000204634| ENSE00001471890|
    |3   2  | 101038461| 101038655|      -| ENSG00000204634| ENSE00001471887|
    |4   2  | 101037532| 101037708|      -| ENSG00000204634| ENSE00001471883|
    |5   2  | 101036018| 101036168|      -| ENSG00000204634| ENSE00001471881|
    |6   2  | 101033544| 101033758|      -| ENSG00000204634| ENSE00001471879|
    +-------+----------+----------+-------+----------------+----------------+


Feature Counts with SeQuiLa
***************************

.. code-block:: R

  #query for count reads
  FC <- sql('SELECT sampleId, Gene_id, Chr ,targets.Start ,targets.End ,Strand, count(*) AS Counts
		        FROM reads JOIN targets
		          ON (Chr=reads.contigName
		          AND reads.end >= CAST(targets.Start AS INTEGER)
		          AND reads.start <= CAST(targets.End AS INTEGER))
		        GROUP BY SampleId, Gene_id, Chr ,targets.Start ,targets.End ,Strand ')


  #preparation data to proper format for further analysis
  tabC <- sum(pivot(groupBy(FC,"Gene_id"),"SampleId"),"Counts")
  head(tabC)

.. code-block:: bash

  #Table with counts of reads for a given sample
  +---------------+--------------+-------------+-------------+-------------+-------------+-------------+
  |       Gene_id | Sub_SRR057629|Sub_SRR057630|Sub_SRR057631|Sub_SRR057632|Sub_SRR057633|Sub_SRR057634|
  +---------------+--------------+-------------+-------------+-------------+-------------+-------------+
  |ENSG00000130054|            31|           30|          147|           39|          230|           16|
  |ENSG00000262692|            NA|           NA|            5|           NA|            7|            1|
  |ENSG00000268673|            12|            4|           18|            5|            4|           14|
  |ENSG00000239881|             6|            3|           19|            9|           17|            9|
  |ENSG00000198015|           143|          135|          304|          213|          371|          133|
  |ENSG00000220924|             2|            5|           17|           10|           15|            5|
  |ENSG00000105707|            95|          145|         3555|         1331|         5107|          250|
  |ENSG00000163406|           669|           67|         1551|          236|          276|          367|
  |ENSG00000236554|             2|            1|            9|            4|            5|            2|
  |ENSG00000233380|           115|           63|          749|          157|         1046|          112|
  +---------------+--------------+-------------+-------------+-------------+-------------+-------------+


DEG analysis with edgeR
************************

.. code-block:: R

    library(edgeR)

    #transform SparkR DataFrame to R data.frame
    tabC <- collect(tabC)

    #input data preparation
    tab1<- as.matrix(apply(tabC,2,as.numeric))
    row.names(tab1) <- tabC$Gene_id
    tab1<- tab1[,-1]
    tab1[is.na(tab1)] <- 0

    #filtering out lowly expressed genes
    isexpr <- rowSums(cpm(tab1) > 5) >= 2
    dane1 <- tab1[isexpr,]

    #grouping factor about samples
    group <- L1
    design <- model.matrix(~group)

    #Normalization and test for DE genes
    y <- DGEList(dane1, group)
    y <- calcNormFactors(y)
    y <- estimateDisp(y,design)
    y <- estimateCommonDisp(y,design)
    y <- estimateTagwiseDisp(y,design)

    et <- exactTest(y)
    #list of top differential expression genes
    topTags(et)

.. code-block:: bash

  +---------------------------------------------------------------+
  |                 Comparison of groups:  N-C                    |
  +---------------------------------------------------------------+
  |               |   logFC |   logCPM|       PValue|          FDR|
  +---------------------------------------------------------------+
  |ENSG00000163735| 3.525597| 2.775419| 4.046946e-10| 8.281266e-06|
  |ENSG00000137441| 3.287043| 2.666538| 8.747908e-10| 8.950422e-06|
  |ENSG00000173432| 5.060981| 6.000767| 3.499855e-09| 2.387251e-05|
  |ENSG00000007062| 3.125545| 4.038641| 5.296368e-09| 2.709489e-05|
  |ENSG00000255071| 4.853109| 5.638364| 1.588535e-08| 6.501240e-05|
  |ENSG00000134339| 4.997591| 5.568151| 2.444644e-08| 8.337459e-05|
  |ENSG00000064886| 3.354718| 4.552207| 3.406488e-08| 9.958139e-05|
  |ENSG00000148346| 3.309200| 6.420025| 6.076321e-08| 1.554247e-04|
  |ENSG00000166787| 5.503744| 1.702660| 7.132882e-08| 1.621780e-04|
  |ENSG00000163220| 3.191790| 3.933292| 7.953153e-08| 1.627454e-04|
  +---------------------------------------------------------------+


DEG analysis with DESeq2
****************************************

.. code-block:: R

    library(DESeq2)

    #input data preparation
    coldata <- matrix(data=L1,nrow=dim(tab1)[2],ncol=1)
    rownames(coldata) <- colnames(tab1)
    colnames(coldata) <- "condition"

    dds <- DESeqDataSetFromMatrix(countData = tab1,
                              colData = coldata,
                              design = ~ condition)

    dds <- DESeq(dds)
    res <- results( dds )
    res <- res[order(res$padj),]

    resSig <- res[ which(res$padj < 0.1 ), ]

    #order results by padj value (most significant to least)
    head( resSig[ order( resSig$log2FoldChange ), ] )
    tail( resSig[ order( resSig$log2FoldChange ), ] )

    #plots to get a sense of what the RNAseq data looks like based on DESEq2 analysis
    plotMA( res, ylim = c(-5, 5) )


.. figure:: plotMA.*
   :align: center

.. code-block:: R

    plotDispEsts( dds, ylim = c(1e-6, 1e1) )


.. figure:: plotDispEsts.*
   :align: center


.. code-block:: R


    hist( res$pvalue, breaks=20, col="grey" )


.. figure:: RplotHist.*
   :align: center

.. code-block:: R


    rld <- rlog( dds )
    head( assay(rld) )


.. code-block:: bash


  +------------------------------------------------------------------------------------------------+
  |               | Sub_SRR057629| Sub_SRR057630| Sub_SRR057631| Sub_SRR057632| Sub_SRR057633| ... |
  +------------------------------------------------------------------------------------------------+
  |ENSG00000059588|      9.081906|      9.388193|      9.192781|      8.932784|      8.837007|     |
  |ENSG00000176209|      7.197959|      6.643900|      6.894974|      7.212627|      7.201518|     |
  |ENSG00000197937|      6.409555|      6.641634|      6.047034|      6.692993|      5.768185|     |
  |ENSG00000105707|      8.320453|      8.918541|     10.611984|     10.502044|     10.943563|     |
  |ENSG00000143013|      9.019995|      8.648218|      8.571342|      8.599573|      8.476145|     |
  |ENSG00000163406|      9.826374|      8.072602|      9.605461|      8.656228|      8.019128|     |
  |     ...                                                                                        |
  +------------------------------------------------------------------------------------------------+


    library( "genefilter" )

    topVarGenes <- head( order( rowVars( assay(rld) ), decreasing=TRUE ), 20 )
    topDESeq2 <- rownames(tab1[topVarGenes,])

.. code-block:: bash

    +--------------------------------------------------------------------------------+
    |                                 topVarGenes                                    |
    +--------------------------------------------------------------------------------+
    | ENSG00000163810 ENSG00000229314 ENSG00000096006 ENSG00000235845 ENSG00000134438|
    | ENSG00000158258 ENSG00000134339 ENSG00000165794 ENSG00000173432 ENSG00000167332|
    | ENSG00000075043 ENSG00000167653 ENSG00000136155 ENSG00000255071 ENSG00000206072|
    | ENSG00000186526 ENSG00000159337 ENSG00000012223 ENSG00000175832 ENSG00000197674|
    +--------------------------------------------------------------------------------+


------------------------------------------------


Simple FeatureCounts
####################

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


Simple Multisample analyses
###########################


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
    biodatageeks/bdg-sequila bdg-shell


.. code-block:: scala

    val tableNameBAM = "reads"
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


.. code-block:: scala

    case class Region(contigName:String,start:Int,end:Int)
     val targets = spark
      .sqlContext
      .createDataFrame(Array(Region("chr1",20138,20294)))
    targets
      .createOrReplaceTempView("targets")

    val query ="""SELECT sampleId,targets.contigName,targets.start,targets.end,count(*)
              FROM reads JOIN targets
        |ON (
        |  targets.contigName=reads.contigName
        |  AND
        |  reads.end >= targets.start
        |  AND
        |  reads.start <= targets.end
        |)
        |GROUP BY sampleId,targets.contigName,targets.start,targets.end
        |having contigName='chr1' AND    start=20138 AND  end=20294""".stripMargin

    val fc = spark
    .sql(query)

    fc.show

.. code-block:: bash

    +--------+----------+-----+-----+--------+
    |sampleId|contigName|start|  end|count(1)|
    +--------+----------+-----+-----+--------+
    | NA12879|      chr1|20138|20294|    1484|
    | NA12878|      chr1|20138|20294|    1484|
    +--------+----------+-----+-----+--------+

.. code-block:: scala

    fc
    .orderBy("sampleId")
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", "\t")
    .csv("/data/input/fc.txt")