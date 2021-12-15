
---
title: "General"
linkTitle: "General"
weight: 1
description: >
   General parameters
---

| Property Name                           | Default         | Meaning                                                                            |
|-----------------------------------------|-----------------|------------------------------------------------------------------------------------|
| spark.biodatageeks.readAligment.method  | hadoopBAM       | Whether to use [`hadoopBAM`](https://github.com/HadoopGenomics/Hadoop-BAM) or [`disq`](https://github.com/disq-bio/disq). Hadoop-BAM seems to be more performant in local mode but has worse horizontal scalability characteristic(~50+ executors). |
| spark.biodatageeks.bam.useGKLInflate    | true            | Whether to use Intel GKL deflater when using HadoopBAM (for disq it is always on)  |
| spark.biodatageeks.bam.validation | SILENT | Method of BAM records [validation](https://github.com/samtools/htsjdk/blob/master/src/main/java/htsjdk/samtools/ValidationStringency.java)|
|
