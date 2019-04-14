

Use cases
=========

---------------------------------------------------

DNA-seq analysis
##########################################
Analysis of Whole Exome Sequencing data to detect Copy Number Variants using SeQuiLa and CODEX (https://www.bioconductor.org/packages/devel/bioc/html/CODEX.html).
Dataset (20 samples) was downloaded from 1000 genomes project (ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/).


.. figure:: SeQuiLa_codex.*
   :scale: 70%
   :align: center

   
--------------------------------------

Run bdg-sequilaR docker 
***************************

.. code-block:: bash

    docker pull biodatageeks/|project_name|:|version|
    docker run -e USERID=$UID -e GROUPID=$(id -g) -it -v /data/samples/1000genomes/:/data \
    -p 4041:4040 biodatageeks/|project_name|:|version| bdg-sequilaR


Download input data, install and load R libraries
*************************************************

.. code-block:: R

    dataDir <- "/data/"

    # Install missing packages from CRAN
    list.of.packages <- c("parallel", "data.table", "reshape", "dplyr")
    new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
    if(length(new.packages)) install.packages(new.packages)

    # Install missing packages from Bioconductor
    biocLitePackages <- c("CODEX") 
    new.biocLitePackage <- biocLitePackages[!(biocLitePackages %in% installed.packages()[,"Package"])]
    if(length(new.biocLitePackage)) { source("http://bioconductor.org/biocLite.R"); biocLite(new.biocLitePackage)}

    # Load packages
    library(sequila); library(parallel); library(data.table); library(reshape); library(dplyr); library(CODEX)

    # Download data
    mc.cores=20
    sampleNames <- paste0("HG0",c(1840:1853,1855,1857:1861))
    mclapply(sampleNames, function(sampleName){
    if (sampleName %in% c("HG01860", "HG01861")){
    download.file(paste0("ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/",
                        sampleName,"/exome_alignment/",
                        sampleName,".chrom20.ILLUMINA.bwa.KHV.exome.20121211.bam"), 
                        paste0(dataDir,sampleName, ".bam"))}
    else{ download.file(paste0("ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/",
                        sampleName,"/exome_alignment/",
                        sampleName,".chrom20.ILLUMINA.bwa.KHV.exome.20120522.bam"), 
                        paste0(dataDir,sampleName, ".bam"))}
    
    }, mc.cores=mc.cores)
    
    # Download exome capture targets
    download.file("ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/exome_pull_down_targets/20130108.exome.targets.bed", 
                    paste0(dataDir,"20130108.exome.targets.bed" ) )
    
    system("sed   's/^chr//' /data/20130108.exome.targets.bed > /data/cleaned_targets.bed")
    
    # Download RefSeq genes track from UCSC
    download.file("http://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/refFlat.txt.gz", 
                    paste0(dataDir, "refFlat.txt.gz"))
    system( paste0("gunzip ",dataDir, "refFlat.txt.gz"))


Load input data to SeQuiLa
***************************

.. code-block:: R     
     
    #Set Spark parameters and connect
    driver_mem <- "40g"
    master <- "local[20]"
    ss<-sequila_connect(master,driver_memory<-driver_mem)

    #create db
    sequila_sql(ss,query="CREATE DATABASE sequila")
    sequila_sql(ss,query="USE sequila")

    #create a BAM data source with reads
    sequila_sql(ss,'reads','CREATE TABLE reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path "/data/*bam")')

    # Check out the reads
    sequila_sql(ss, query= "select * from reads limit 10")

.. code-block:: bash

    # Source:   table<test> [?? x 10]
    # Database: spark_connection
    sampleId contigName start   end cigar  mapq baseq reference flags materefind
    <chr>    <chr>      <int> <int> <chr> <int> <chr> <chr>     <int>      <int>
    1 HG01840  20         60123 60212 90M      60 9BEB~ 20           99         19
    2 HG01840  20         60206 60273 68M2~    60 989E~ 20           99         19
    3 HG01840  20         60260 60349 90M      60 B>C=~ 20          147         19
    4 HG01840  20         60297 60386 90M      60 ;C?>~ 20          147         19
    5 HG01840  20         60687 60776 90M      60 :E=G~ 20           99         19
    6 HG01840  20         60780 60869 90M      60 9@C?~ 20          163         19
    7 HG01840  20         60841 60930 90M      29 9=>E~ 20          163         19
    8 HG01840  20         60843 60932 90M      60 9C8D~ 20           99         19
    9 HG01840  20         60882 60971 90M      60 9B@@~ 20           99         19
    10 HG01840  20         60889 60959 19S7~    29 8<A6~ 20           99         19
    # ... with more rows

.. code-block:: R

    #create a table with target data 
    sequila_sql(ss,'targets','CREATE TABLE targets (Chr string, Start integer,End integer, v1 string)
    USING csv
    OPTIONS (path "/data/cleaned_targets.bed", header "false", inferSchema "false", delimiter "\t")')
    
    
    #inspect content of targets table
    sequila_sql(ss, query= "select * from targets limit 10")

.. code-block:: bash

    # Source:   table<test> [?? x 4]
    # Database: spark_connection
    Chr    Start    End v1   
    <chr>  <int>  <int> <chr>
    1 1      14642  14882 NA   
    2 1      14943  15063 NA   
    3 1      15751  15990 NA   
    4 1      16599  16719 NA   
    5 1      16834  17074 NA   
    6 1      17211  17331 NA   
    7 1      30275  30431 NA   
    8 1      69069  70029 NA   
    9 1     129133 129253 NA   
    10 1     228233 228354 NA   
    # ... with more rows


Count the number of reads per target using SeQuiLa
**************************************************

.. code-block:: R

    query <- "SELECT SampleId, Chr ,targets.Start ,targets.End ,CAST(targets.End AS INTEGER)-
                               CAST(targets.Start AS INTEGER) + 1 AS Length, count(*) AS Counts 
                FROM reads 
                JOIN targets ON (Chr=reads.contigName AND reads.end >= CAST(targets.Start AS INTEGER)
                                                      AND reads.start <= CAST(targets.End AS INTEGER)) 
                GROUP BY  SampleId, Chr, targets.Start, targets.End"

::

     Note that you can easily modify a query to filter out low quality reads (e.g., add 'mapq > 20' to WHERE clause).
     
.. code-block:: R

    # Collect results
    res <- sequila_sql(ss,'results',query)
    readCountPerTarget <-  collect(res)
    head(readCountPerTarget)

.. code-block:: bash

    SampleId Chr  Start    End Length Counts
    1:  HG01840   1  14642  14882    241      3
    2:  HG01840   1 741165 741285    121    395
    3:  HG01840   1 881703 881973    271    183
    4:  HG01840   1 897196 897436    241     67
    5:  HG01840   1 898040 898310    271     32
    6:  HG01840   1 901892 902012    121     55



Run CODEX
***************************

.. code-block:: R

    # Transform read count data to matrix
    chr <- "20"
    readCountPerTarget$key <- paste0(readCountPerTarget$Chr, ":", readCountPerTarget$Start, "_", readCountPerTarget$End)
    Y <- dcast(data.table(readCountPerTarget), key ~ SampleId, value.var="Counts")
    Y[is.na(Y)] <- 1 
    rownames(Y) <- 1:nrow(Y)
    keys <- Y$key 
    Y <- Y[,-1,with=F] # remove first column (key)
    targets <- data.frame(do.call(rbind, strsplit(keys,"[:_]")), stringsAsFactors=F)
    colnames(targets) <- c("Chr", "Start", "Stop")
    
    #Sort targets and Y matrix
    ord <- order(targets$Chr, as.numeric(targets$Start), as.numeric(targets$Stop))
    targets <- targets[ord, ];  Y <- Y [ord, ]
    idx <- which(targets$Chr == chr)
    Y <- as.matrix(Y[idx,])
    targetsChr <- targets[idx,]
    ref <- IRanges(start = as.numeric(targetsChr$Start), end = as.numeric(targetsChr$Stop))

    #Perform Qualty Control
    gc <- getgc(chr, ref)
    mapp <- getmapp(chr, ref)
    mapp_thresh <- 0.9 # remove exons with mapability < 0.9
    cov_thresh_from <- 20 # remove exons covered by less than 20 reads
    cov_thresh_to <- 4000 #  remove exons covered by more than 4000 reads
    length_thresh_from <- 20 # remove exons of size < 20
    length_thresh_to <- 2000 # remove exons of size > 2000
    gc_thresh_from <- 20 # remove exons with GC < 20
    gc_thresh_to <- 80 # or GC > 80
    sampname <- colnames(Y)
    qcObj <- qc(Y, sampname, chr, ref, mapp, gc, 
                cov_thresh = c(cov_thresh_from, cov_thresh_to), 
                length_thresh = c(length_thresh_from, length_thresh_to), 
                mapp_thresh = mapp_thresh, 
                gc_thresh = c(gc_thresh_from, gc_thresh_to))
    Y_qc <- qcObj$Y_qc; sampname_qc <- qcObj$sampname_qc; gc_qc <- qcObj$gc_qc
    mapp_qc <- qcObj$mapp_qc; ref_qc <- qcObj$ref_qc; qcmat <- qcObj$qcmat

    # Normalization           
    normObj <- normalize(Y_qc, gc_qc, K = 1:9)
    Yhat <- normObj$Yhat; AIC <- normObj$AIC; BIC <- normObj$BIC
    RSS <- normObj$RSS; K <- normObj$K
    optK=which.max(BIC)

    # Segmentation
    finalcall <- CODEX::segment(Y_qc, Yhat, optK = optK, K = K, sampname_qc,   ref_qc, chr, lmax = 200, mode = "integer")
    finalcall <- data.frame(finalcall, stringsAsFactors=F)
    finalcall$targetCount <- as.numeric(finalcall$ed_exon) - as.numeric(finalcall$st_exon)
    finalcall$chr <- paste0("chr", finalcall$chr)

    # Save results to csv file
    write.csv(finalcall, file="/data/cnv_results.csv", row.names=F, quote=F)
    
    
    # Plot detected CNVs encompassing more than 3 targets (only one duplication found). 
    plotCall <- function(calls,i , Y_qc, Yhat_opt){
    startIdx <- as.numeric(calls$st_exon[i])
    stopIdx <- as.numeric(calls$ed_exon[i])
    sampleName <- calls$sample_name[i]
    wd <- 20
    startPos <- max(1,(startIdx-wd))
    stopPos <- min((stopIdx+wd), nrow(Y_qc))
    selQC <- Y_qc[startPos:stopPos,]
    selQC[selQC ==0] <- 0.00001
    selYhat <- Yhat_opt[startPos:stopPos,]
    matplot(matrix(rep(startPos:stopPos, ncol(selQC)), ncol=ncol(selQC)), log(selQC/selYhat,2), type="l",lty=1, col="dimgrey",  lwd=1, xlab="exon nr", ylab="logratio(Y/Yhat)")
    lines(startPos:stopPos,log( selQC[,sampleName]/ selYhat[,sampleName],2), lwd=3, col="red")
    }

    
    plotCall (finalcall, which(finalcall$targetCount > 3), Y_qc, Yhat[[optK]])
    
    
.. figure:: PipelineCNVSeqWithSequila.*
   :scale: 40%
   :align: center

Annotate detected CNVs with overlapping genes using SeQuiLa
***********************************************************

.. code-block:: R
    
    # Load detected CNVs into the database
    sequila_sql(ss,'cnv_results','CREATE TABLE cnv_results 
                                    USING csv
                                    OPTIONS (path "/data/cnv_results.csv", header "true", inferSchema "true", delimiter ",")')

                                    
    
    # Load gene coordinates (from refFlat file)
    sequila_sql(ss,'ref_flat','CREATE TABLE ref_flat  (symbol string, id string,chr string, strand string, txstart integer, txend integer, 
                                                        cdsstart integer, cdsend integer, exonnum integer, exonstarts string, exonends string )
                                USING csv
                                OPTIONS (path "/data/refFlat.txt", header "false", inferSchema "false", delimiter "\t")')

                                

    # Find genes overlapping each CNV
    query1 <- "SELECT sample_name,cnv_results.chr, cnv,st_bp, ed_bp, length_kb, st_exon, ed_exon, raw_cov, norm_cov, copy_no, lratio, mBIC, targetCount, collect_set(symbol) as Genes
            FROM cnv_results JOIN ref_flat
                                ON (cnv_results.chr=ref_flat.chr AND ref_flat.txend >= cnv_results.st_bp
                                AND ref_flat.txstart  <= cnv_results.ed_bp) 
            GROUP BY sample_name, cnv_results.chr, cnv,st_bp, ed_bp, length_kb, st_exon, ed_exon, raw_cov, norm_cov, copy_no, lratio, mBIC, targetCount"

    annotatedCalls <- data.frame(collect(sequila_sql(ss,'annotatedCalls',query1)))
    head(annotatedCalls)
    
.. code-block:: bash

    sample_name   chr cnv    st_bp    ed_bp length_kb st_exon ed_exon raw_cov norm_cov copy_no  lratio    mBIC targetCount                      Genes
    1     HG01846 chr20 del  1895652  1902379     6.728     118     119    1663     2553       1 123.879 104.897           1                      SIRPA
    2     HG01844 chr20 dup  1638233  1896162   257.930     117     118    3328     2304       3 197.332 359.543           1 SIRPA, SIRPG, LOC100289473
    3     HG01861 chr20 del  1895652  1902379     6.728     118     119    1506     2127       1  19.696  85.584           1                      SIRPA
    4     HG01858 chr20 del 26061800 26063616     1.817    1229    1230     445      704       1  43.572  23.580           1                    FAM182A
    5     HG01851 chr20 del  1895652  1902379     6.728     118     119    1344     1913       1  24.977   5.995           1                      SIRPA
    6     HG01848 chr20 dup  1584562  1592181     7.620     110     111      89       39       5  23.047 942.548           1                     SIRPB1



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

Run bdg-sequilaR docker
***************************

.. code-block:: bash


    docker pull biodatageeks/|project_name|:|version|

    docker run -p 4041:4040  -e USERID=$UID -e GROUPID=$(id -g) \
    -it  -v /Users/ales/data/sequila:/data/input biodatageeks/|project_name|:|version| bdg-sequilaR


Load input data to SeQuila
*************************************************
.. code-block:: R

    install.packages('devtools')
    devtools::install_github('ZSI-Bio/bdg-sparklyr-sequila')

    library(sparklyr)
    library(sequila)
    library(dplyr)
    library(reshape2)

    #Set Spark parameters and connect
    driver_mem <- "40g"
    master <- "local[20]"
    ss<-sequila_connect(master,driver_memory<-driver_mem)

    #create db
    sequila_sql(ss,query="CREATE DATABASE sequila")
    sequila_sql(ss,query="USE sequila")

    #create a BAM data source with reads
    sequila_sql(ss,'reads','CREATE TABLE reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path "/data/*bam")')

    # Check out the reads
	sequila_sql(ss, query="select count(distinct sampleId) from reads"))

.. code-block:: bash

      +-------------------------+
      |count(DISTINCT sampleId) |
      +-------------------------+
      |                      30 |
      +-------------------------+

.. code-block:: R

    #GTF with target regions
    system(" awk -v OFS='\t' '{if ($3~/gene/); print  $1, $4, $5, $7,substr($10,2,15)}' /Users/ales/data/sequila/Homo_sapiens.gtf > /Users/ales/data/sequila/Homo_sapiens_genes.gtf ")

    sequila_sql(ss,'targets','CREATE TABLE targets(Chr string, Start integer, End integer, Strand string, Gene_id string)
            USING csv
            OPTIONS (path "/Users/ales/data/sequila/Homo_sapiens_genes.gtf", header "false", inferSchema "false", delimiter "\t")')

    sequila_sql(ss, query= "select * from targets limit 10")

.. 	code-block:: bash

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


Count the number of reads per target using SeQuiLa
**************************************************

.. code-block:: R

  #query for count reads
  query <- 'SELECT sampleId, Gene_id, Chr ,targets.Start ,targets.End ,Strand, count(*) AS Counts
  FROM reads JOIN targets
  ON (Chr=reads.contigName
  AND reads.end >= CAST(targets.Start AS INTEGER)
  AND reads.start <= CAST(targets.End AS INTEGER))
  GROUP BY SampleId, Gene_id, Chr ,targets.Start ,targets.End ,Strand '

  res <- sequila_sql(ss,'results',query)
  readCountPerGene <-  collect(res)
  head(readCountPerGene)

  #preparation data to proper format for further analysis
  tabC <- dcast(readCountPerGene, Gene_id ~ sampleId, value.var="Counts", fun.aggregate=sum)

  head(tabC)

.. code-block:: bash

  #Table with counts of reads for a given sample
  +---------------+--------------+-------------+-------------+-------------+-------------+-------------+
  |       Gene_id | Sub_SRR057629|Sub_SRR057630|Sub_SRR057631|Sub_SRR057632|Sub_SRR057633|Sub_SRR057634|
  +---------------+--------------+-------------+-------------+-------------+-------------+-------------+
  |ENSG00000130054|            31|           30|          147|           39|          230|           16|
  |ENSG00000262692|            10|           10|            5|           17|            7|            1|
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

    #input data preparation
    tab1<- as.matrix(apply(tabC,2,as.numeric))
    row.names(tab1) <- tabC$Gene_id
    tab1<- tab1[,-1]
    tab1[is.na(tab1)] <- 0

    #filtering out lowly expressed genes
    isexpr <- rowSums(cpm(tab1) > 5) >= 2
    dane1 <- tab1[isexpr,]

    L1 <- as.factor(c("C","C","C","C","C","C","C","C","C","C","C","C","C","C","C","C","C","C","C","C","N","N","N","N","N","N","N","N","N","N"))

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


Nanopore long reads from WGS analyses
#####################################

.. code-block:: bash

    imwiewior@cdh00:/data/work/nanopore_bam/minimap> hdfs dfs  -du -h  /data/granges/nanopore/* | grep sorted
    130.9 G  261.9 G  /data/granges/nanopore/NA12878-Albacore2.1.sorted.bam
    126.5 G  253.0 G  /data/granges/nanopore/rel5-guppy-0.3.0-chunk10k.sorted.bam
    51.2 M   102.4 M  /data/granges/nanopore/rel5-guppy-0.3.0-chunk10k.sorted.bam.bai



.. code-block:: scala

  import org.apache.spark.sql.SequilaSession
  import org.biodatageeks.utils.{SequilaRegister, UDFRegister,BDGInternalParams}


  val ss = SequilaSession(spark)
  SequilaRegister.register(ss)
  /*enable disq support*/
  ss.sqlContext.setConf("spark.biodatageeks.readAligment.method", "disq")

  /* WGS -bases-blocks*/
  ss.sql("""
  CREATE TABLE IF NOT EXISTS reads_nanopore
  USING org.biodatageeks.datasources.BAM.BAMDataSource
  OPTIONS(path '/data/granges/nanopore/*sorted*.bam')
  """.stripMargin)

  ss.sql("select distinct sampleId from reads_nanopore").show

    +--------------------------------+
    |                        sampleId|
    +--------------------------------+
    |      NA12878-Albacore2.1.sorted|
    |rel5-guppy-0.3.0-chunk10k.sorted|
    +--------------------------------+


  /*Albacore mapper*/
  spark.time{
  ss.sql(s"SELECT * FROM bdg_coverage('reads_nanopore','NA12878-Albacore2.1.sorted', 'blocks')").write.format("parquet").save("/tmp/NA12878-Albacore2.1.sorted.parquet")}

  /*guppy mapper*/
  spark.time{
  ss.sql(s"SELECT * FROM bdg_coverage('reads_nanopore','rel5-guppy-0.3.0-chunk10k.sorted', 'blocks')").write.format("parquet").save("/tmp/rel5-guppy-0.3.0-chunk10k.sorted.parquet")}
