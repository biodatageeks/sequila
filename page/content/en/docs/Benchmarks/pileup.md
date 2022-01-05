
---
title: "Pileup and coverage"
linkTitle: "Pileup and coverage"
weight: 1
description: >
    SeQuiLa's Pileup and coverage benchmarks
---

## Testing environment

### Datasets
|Type| File      | Size [GB]  |Download| 
|----|-----------|--------|--------|
|ES|NA12878.proper.wes.md.bam| 17 |[BAM](https://storage.googleapis.com/biodatageeks/sequila/data/WES/NA12878.proper.wes.md.bam) [BAI](https://storage.googleapis.com/biodatageeks/sequila/data/WES/NA12878.proper.wes.md.bam.bai) [SBI](https://storage.googleapis.com/biodatageeks/sequila/data/WES/NA12878.proper.wes.md.bam.sbi)|
|WGS|NA12878.proper.wgs.md.bam| 278|[BAM](https://storage.googleapis.com/biodatageeks/sequila/data/WGS/NA12878.proper.wgs.md.bam) [BAI](https://storage.googleapis.com/biodatageeks/sequila/data/WGS/NA12878.proper.wgs.md.bam.bai) [SBI](https://storage.googleapis.com/biodatageeks/sequila/data/WGS/NA12878.proper.wgs.md.bam.sbi)|
|Reference|Homo_sapiens_assembly18.fasta| 2.9 | [FASTA](https://storage.googleapis.com/biodatageeks/sequila/data/reference/Homo_sapiens_assembly18.fasta) [FAI](https://storage.googleapis.com/biodatageeks/sequila/data/reference/Homo_sapiens_assembly18.fasta.fai)|

### Tools

| Software | Version | Pileup | Coverage | Multi-threaded | Distributed |
|----------|---------|--------|----------|----------------|-------------|
| [ADAM](https://github.com/bigdatagenomics/adam)| 0.36.0| no | yes| yes | yes |
| [GATK](https://github.com/broadinstitute/gatk)|4.2.3.0 | yes | yes(only intervals) | no| no |
| [GATK-Spark](https://github.com/broadinstitute/gatk)|4.2.3.0 | yes| yes(only intervals)| yes| yes |
| [megadepth](https://github.com/ChristopherWilks/megadepth)| 1.1.1| no |yes | yes(only I/O) | no|
| [mosdepth](https://github.com/brentp/mosdepth)| 0.3.2 | no| yes |  yes(only I/O) | no |
| [sambamba](https://github.com/biod/sambamba)| 0.8.1 |no | yes| no | no |
| [samtools](https://github.com/samtools/samtools)| 1.9 | yes | yes | no | no |
| [samtools](https://github.com/samtools/samtools)| 1.14 | yes | yes | yes(only coverage, I/O) | no |
| [SeQuiLa-cov](https://github.com/biodatageeks/sequila)| 0.6.11 | no | yes | yes| yes |
| [SeQuiLa](https://github.com/biodatageeks/sequila)| 1.0.0 | yes | yes | yes| yes |

### Single node specification

| Processor | Base freq [GHz] | CPUs | Total cores | Memory [GB] | OS Version | Disk|
|-----------|-----------------|------|-------------|-------------|------------|-----|
|Intel(R) Xeon(R) E5-2618L v4 |2.20| 2| 20(40) |256 | RHEL 7.8(Maipo) |3TB(RAID1)|

### Hadoop cluster

| Masters |Workers | Hadoop distribution| Total disk HDFS [TB] | Total YARN cores | Total YARN RAM [TB] | Net [Gbits]|
|---------|--------|--------------------|----------------------|-------------|-------------|------------|
|6| 34 |HDP 3.1.4 |700|1360(680)|6.8 | 100| 

####  SeQuiLa, ADAM and GATK(Spark) parameters

| Parameter | Value | SeQuiLa only| Local-test| Cluster-test|
|-----------|--------|------------|------|--------|
|spark.biodatageeks.pileup.useVectorizedOrcWriter| true | yes | yes |no |
|spark.sql.orc.compression.codec | snappy | no | yes | yes |
|spark.biodatageeks.readAligment.method| hadoopBAM| yes | yes | no |
|spark.biodatageeks.readAligment.method| disq| yes | no | yes |
|spark.serializer| org.apache.spark.serializer.KryoSerializer| yes[^1] | yes | yes |
|spark.kryo.registrator| org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator| yes | yes | yes|
|spark.kryoserializer.buffer.max|1024m| yes| yes | yes |
|spark.hadoop.mapred.min.split.size|268435456| yes| no | yes|
|spark.hadoop.mapred.min.split.size|134217728| yes| yes | no|
|spark.driver.memory| 8g| no | no | yes |
|spark.driver.memory| 16g| no | yes | no |
|spark.executor.memory| 4g[^2]| no | yes | yes |
|spark.dynamicAllocation.enabled|false|no|yes|yes|

[^1]: GATK-Spark and ADAM also use fast serialization using Kryo lib and custom registrators.
[^2]: In case of GATK and WGS tests needed to be increased to 8g

####  Other parameters

| Software | Operation|Command line options |Multithreading options|
|-----------|--------|--------|-----|
| ADAM| coverage |`-- coverage -collapse`|`--spark-master local[$n]`|
| GATK| pileup |`PileupSpark`|`--spark-master local[$n]`|
| GATK| pileup |`Pileup`|--|
|megadepth|coverage| `--coverage --require-mdz --keep-order`|  `--threads $(n-1)`|
|mosdepth|coverage|`-x`| `--threads $(n-1)`|
|sambamba|coverage|`depth base`|--nthreads=$n[^3]|
|samtools|coverage|`depth`|`--threads $(n-1)`[^4]|
|samtools|pileup|`mpileup -B -x  -A -q 0 -Q 0`|--|
[^3]: Does not result in increasing parallelism level
[^4]: Available in version >= 1.13

### SeQuiLa

#### Coverage
```scala
import org.apache.spark.sql.{SequilaSession, SparkSession}
val ss = SequilaSession(spark)
ss.sparkContext.setLogLevel("INFO")
val bamPath = "/scratch/wiewiom/WGS/NA12878.proper.wgs.md.bam"
val referencePath = "/home/wiewiom/data/Homo_sapiens_assembly18.fasta"
ss.time{
    ss
    .coverage(bamPath, referencePath)
    .write
    .orc("/tmp/sequila.coverage")
}
```

#### Pileup
```scala
import org.apache.spark.sql.{SequilaSession, SparkSession}
val ss = SequilaSession(spark)
ss.sparkContext.setLogLevel("INFO")
val bamPath = "/scratch/wiewiom/WGS/NA12878.proper.wgs.md.bam"
val referencePath = "/home/wiewiom/data/Homo_sapiens_assembly18.fasta"
ss.time{
    ss
    .pileup(bamPath, referencePath, true)
    .write
    .orc("/tmp/sequila.pileup")
}
```


### Results single node
#### Coverage
{{< figure src="../coverage-single-node.png" >}}

#### Pileup
{{< figure src="../pileup-single-node.png" >}}

### Results Hadoop cluster
#### Coverage
{{< figure src="../coverage-cluster.png" >}}

#### Pileup
{{< figure src="../pileup-cluster.png" >}}