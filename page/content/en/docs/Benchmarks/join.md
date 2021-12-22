
---
title: "Interval joins"
linkTitle: "Interval joins"
weight: 3
description: >
    SeQuiLa's interval joins benchmark
---


## Performance tests description

In order to evaluate our range join strategy we have run a number of tests using both one-node and a Hadoop cluster
installations. In that way we were able to analyze both vertical (by means of adding computing resources such as CPU/RAM on one machine)
as well as horizontal (by means of adding resources on multiple machines) scalability.

The main idea of the test was to compare performance of SeQuiLa with other tools like featureCounts and genAp that can be used
to compute the number of reads intersecting predefined genomic intervals. It is by no means one of the most commonly used operations
in both DNA and RNA-seq data processing, most notably in gene differential expression and copy number variation-calling.
featureCounts performance results have been treated as a baseline. In order to show the difference between the naive approach using the
default range join strategy available in Spark SQL and SeQuiLa interval-tree one, we have included it in the single-node test.

## Testing enviornment


### Datasets
|Test name| Format |Size [GB]|Row count  | 
|---------|--------|---------|-----------|
WES-SN   |  BAM    | 17      | 161544693 |
WES-SN   |   ADAM  |  14     | 161544693 |
WES-SN   |   BED   |  0.0045 | 193557    |
WGS-CL   |   BAM   |  273    | 2617420313|
WGS-CL   |   ADAM  |  215    | 2617420313|
WGS-CL   |   BED   |  0.0016 | 68191|

WES-SN - tests performed on a single node using WES dataset

WGS-CL - tests performed on a cluster using WGS dataset

### Hadoop cluster
| Masters |Workers | Hadoop distribution| Total disk HDFS [TB] | Total YARN cores | Total YARN RAM [TB] | Net [Gbits]|
|---------|--------|--------------------|----------------------|-------------|-------------|------------|
|1| 4 |CDH 5.12 |5|96|256 | 10| 

#### Apache Spark configuration

| Parameter | Value | SeQuiLa only| Local-test| Cluster-test|
|-----------|--------|------------|------|--------|
|spark.driver.memory| 8g| no | yes | yes |
|spark.executor.memory| 4g-8g| no | yes | yes |
|spark.executor.cores| 2-4| no | yes | yes |
|spark.dynamicAllocation.enabled|false|no|yes|yes|

#### Software

| Software | Version |
|-----------|--------|
|Apache Spark| 2.2.1|
|Scala| 2.11.8|
|JDK| 1.8|
|ADAM|0.22|
|featureCounts|1.6.0|

#### SeQuiLa

##### BAM table 

```scala
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
```

##### ADAM table 

```scala
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
```

##### BED table

```scala
    val  bedPath="tgp_exome_hg18.bed"
    spark.sql(s"""
        |CREATE TABLE targets
        |USING org.biodatageeks.sequila.datasources.BED.BEDDataSource
        |OPTIONS (path "file:///${bedPath}")""".stripMargin)
    spark.sql("SELECT contig,post_start, pos_end FROM targets LIMIT 1").show

    +------+---------+--------+
    |contig|pos_start| pos_end|
    +------+---------+--------+
    |  chr1|     4806|    4926|
    +------+---------+--------+
```

```sql
    SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
         ON (targets.contigName=reads.contigName
         AND
         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
         AND
         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
         )
         GROUP BY targets.contigName,targets.start,targets.end
```

### Results single node
{{< figure src="../join-local.png" >}}
### Results Hadoop cluster
{{< figure src="../join-cluster.png" >}}

### Conclusions
SeQuiLa when run in parallel outperforms selected competing tools in terms of speed on single node (1.7-22.1x) and cluster (3.2-4.7x). 
SeQuiLa strategy involving broadcasting interval forest with all data columns (SeQuiLa_it_all) performs best in most of the cases (no network shuffling required), 
whereas broadcasting intervals with identifiers only (SeQuiLa_it_int) performs comparable to, or better than GenAp. 
All algorithms favours columnar (ADAM) to row oriented (BAM) file format due to column pruning and disk I/O operations reduction.


