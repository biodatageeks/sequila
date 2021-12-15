
---
title: "Configuration"
linkTitle: "Configuration"
weight: 3
description: >
    Glossary of SeQuiLa's parameters and configuration options
---
## How to set parameters
{{< tabpane >}}
{{< tab header="SQL" lang="sql" >}}
SET spark.biodatageeks.readAligment.method=disq;
{{< /tab >}}
{{< tab header="Python" lang="python">}}
from pysequila import SequilaSession
ss = SequilaSession \
.builder \
.getOrCreate()
ss.sql("SET spark.biodatageeks.readAligment.method=disq")
{{< /tab >}}
{{< /tabpane >}}

## General

| Property Name                           | Default         | Meaning                                                                            |
|-----------------------------------------|-----------------|------------------------------------------------------------------------------------|
| spark.biodatageeks.readAligment.method  | hadoopBAM       | Whether to use [`hadoopBAM`](https://github.com/HadoopGenomics/Hadoop-BAM) or [`disq`](https://github.com/disq-bio/disq). Hadoop-BAM seems to be more performant in local mode but has worse horizontal scalability characteristic(~50+ executors). |
| spark.biodatageeks.bam.useGKLInflate    | true            | Whether to use Intel GKL deflater when using HadoopBAM (for disq it is always on)  |
| spark.biodatageeks.bam.validation | SILENT | Method of BAM records [validation](https://github.com/samtools/htsjdk/blob/master/src/main/java/htsjdk/samtools/ValidationStringency.java)|
|

## Coverage and Pileup

| Property Name                           | Default         | Meaning                                                                            |
|-----------------------------------------|-----------------|------------------------------------------------------------------------------------|
|spark.biodatageeks.coverage.filterFlag | 1796 | Which SAM flags to use for filtering records. Check [SAM explainer](https://broadinstitute.github.io/picard/explain-flags.html) for details.|
|spark.biodatageeks.coverage.minMapQual| 0| Minimal value of reads' mapping quality to be included in coverage/pileup calculations |
|spark.biodatageeks.pileup.useVectorizedOrcWriter | false | Whether to use vectorized ORC writer and to bypass Spark's InternalRow serialization and write directly to the ouput file. This option is still experimental but speeds up saving results substantially.|
|spark.biodatageeks.pileup.maxBaseQuality | 40 | Specify the number of base quality values - internally used for allocating BQ counters. |


## Interval joins

| Property Name                           | Default         | Meaning                                                                            |
|-----------------------------------------|-----------------|------------------------------------------------------------------------------------|
|spark.biodatageeks.rangejoin.useJoinOrder| false | Whether to always broadcast the right table of a join or to computer row counts and pick up the smaller one. |
|spark.biodatageeks.rangejoin.maxBroadcastSize| 0.1*spark.driver.memory| The maximum allowed size of the broadcasted intverval structure which is used by the SeQuiLa's optimizer to chose interval join [algorithm](http://biodatageeks.ii.pw.edu.pl/sequila/architecture/architecture.html#optimizations).|
|spark.biodatageeks.rangejoin.maxGap| 0 | The maximum gap between between regions |
|spark.biodatageeks.rangejoin.minOverlap| 1 | The minimal length of the overlap between regions |
|spark.biodatageeks.rangejoin.intervalHolderClass|[`IntervalTreeRedBlack`](https://github.com/biodatageeks/sequila/blob/master/src/main/scala/org/biodatageeks/sequila/rangejoins/methods/IntervalTree/IntervalTreeRedBlack.java)| [Pluggable](http://biodatageeks.ii.pw.edu.pl/sequila/architecture/architecture.html#custom-interval-structure) mechanism for implementing custom interval structures.|