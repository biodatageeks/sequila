
---
title: "BAM"
linkTitle: "BAM"
weight: 1
description: >
    SeQuiLa's BAM format support
---

## Examples

```bash
mkdir -p /tmp/data
wget -nc https://github.com/biodatageeks/pysequila/raw/master/features/data/NA12878.multichrom.md.bam -O /tmp/data/NA12878.bam
wget -nc https://github.com/biodatageeks/pysequila/raw/master/features/data/NA12878.multichrom.md.bam.bai -O /tmp/data/NA12878.bam.bai
```


{{< tabpane >}}
{{< tab header="SQL" lang="sql" >}}

CREATE TABLE IF NOT EXISTS reads
USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
OPTIONS(path "/tmp/data/NA12878.bam");

DESCRIBE TABLE reads;

+---------+---------+-------+
| col_name|data_type|comment|
+---------+---------+-------+
|sample_id|   string|   null|
|    qname|   string|   null|
|     flag|      int|   null|
|   contig|   string|   null|
|      pos|      int|   null|
|pos_start|      int|   null|
|  pos_end|      int|   null|
|     mapq|      int|   null|
|    cigar|   string|   null|
|    rnext|   string|   null|
|    pnext|      int|   null|
|     tlen|      int|   null|
|      seq|   string|   null|
|     qual|   string|   null|
|   tag_AM|      int|   null|
|   tag_AS|      int|   null|
|   tag_BC|   string|   null|
|   tag_BQ|   string|   null|
|   tag_BZ|   string|   null|
|   tag_CB|   string|   null|
|   tag_CC|   string|   null|
|   tag_CG|   string|   null|
|   tag_CM|      int|   null|
|   tag_CO|   string|   null|
|   tag_CP|      int|   null|
|   tag_CQ|   string|   null|
|   tag_CR|   string|   null|
|   tag_CS|   string|   null|
|   tag_CT|   string|   null|
|   tag_CY|   string|   null|
|   tag_E2|   string|   null|
|   tag_FI|      int|   null|
|   tag_FS|   string|   null|
|   tag_FZ|   string|   null|
|   tag_H0|      int|   null|
|   tag_H1|      int|   null|
|   tag_H2|      int|   null|
|   tag_HI|      int|   null|
|   tag_IH|      int|   null|
|   tag_LB|   string|   null|
|   tag_MC|   string|   null|
|   tag_MD|   string|   null|
|   tag_MI|   string|   null|
|   tag_MQ|      int|   null|
|   tag_NH|      int|   null|
|   tag_NM|      int|   null|
|   tag_OA|   string|   null|
|   tag_OC|   string|   null|
|   tag_OP|      int|   null|
|   tag_OQ|   string|   null|
|   tag_OX|   string|   null|
|   tag_PG|   string|   null|
|   tag_PQ|      int|   null|
|   tag_PT|   string|   null|
|   tag_PU|   string|   null|
|   tag_Q2|   string|   null|
|   tag_QT|   string|   null|
|   tag_QX|   string|   null|
|   tag_R2|   string|   null|
|   tag_RG|   string|   null|
|   tag_RX|   string|   null|
|   tag_SA|   string|   null|
|   tag_SM|      int|   null|
|   tag_TC|      int|   null|
|   tag_U2|   string|   null|
|   tag_UQ|      int|   null|
+---------+---------+-------+

SELECT qname, contig, pos_start, pos_end, cigar, seq FROM reads LIMIT 5;

+--------------------+------+---------+-------+-----+--------------------+
|               qname|contig|pos_start|pos_end|cigar|                 seq|
+--------------------+------+---------+-------+-----+--------------------+
|61DC0AAXX100127:8...|    MT|        7|    107| 101M|AGGTCTATCACCCTATT...|
|61DC0AAXX100127:8...|    MT|        9|    109| 101M|GTCTGTCACCCTTGTAG...|
|61DC0AAXX100127:8...|    MT|       10|    110| 101M|TCTATCCCCCTATTAAC...|
|61DC0AAXX100127:8...|    MT|       20|    120| 101M|TATTATCCACTCACGGG...|
|61CC3AAXX100125:5...|    MT|       25|    100|  76M|ACCACTCACGGGAGCTC...|
+--------------------+------+---------+-------+-----+--------------------+

SELECT count(*) AS cnt FROM reads;

+-----+
|  cnt|
+-----+
|22607|
+-----+

{{< /tab >}}
{{< tab header="Python" lang="python">}}
>>> from pysequila import SequilaSession
>>> ss = SequilaSession \
.builder \
.getOrCreate()

>>> reads_df=ss.read\
.format("org.biodatageeks.sequila.datasources.BAM.BAMDataSource")\
.load("/tmp/data/NA12878.bam")
>>> reads_df.printSchema()
root
|-- sample_id: string (nullable = true)
|-- qname: string (nullable = true)
|-- flag: integer (nullable = false)
|-- contig: string (nullable = true)
|-- pos: integer (nullable = false)
|-- pos_start: integer (nullable = false)
|-- pos_end: integer (nullable = false)
|-- mapq: integer (nullable = false)
|-- cigar: string (nullable = true)
|-- rnext: string (nullable = true)
|-- pnext: integer (nullable = false)
|-- tlen: integer (nullable = false)
|-- seq: string (nullable = true)
|-- qual: string (nullable = true)
|-- tag_AM: integer (nullable = true)
|-- tag_AS: integer (nullable = true)
|-- tag_BC: string (nullable = true)
|-- tag_BQ: string (nullable = true)
|-- tag_BZ: string (nullable = true)
|-- tag_CB: string (nullable = true)
|-- tag_CC: string (nullable = true)
|-- tag_CG: string (nullable = true)
|-- tag_CM: integer (nullable = true)
|-- tag_CO: string (nullable = true)
|-- tag_CP: integer (nullable = true)
|-- tag_CQ: string (nullable = true)
|-- tag_CR: string (nullable = true)
|-- tag_CS: string (nullable = true)
|-- tag_CT: string (nullable = true)
|-- tag_CY: string (nullable = true)
|-- tag_E2: string (nullable = true)
|-- tag_FI: integer (nullable = true)
|-- tag_FS: string (nullable = true)
|-- tag_FZ: string (nullable = true)
|-- tag_H0: integer (nullable = true)
|-- tag_H1: integer (nullable = true)
|-- tag_H2: integer (nullable = true)
|-- tag_HI: integer (nullable = true)
|-- tag_IH: integer (nullable = true)
|-- tag_LB: string (nullable = true)
|-- tag_MC: string (nullable = true)
|-- tag_MD: string (nullable = true)
|-- tag_MI: string (nullable = true)
|-- tag_MQ: integer (nullable = true)
|-- tag_NH: integer (nullable = true)
|-- tag_NM: integer (nullable = true)
|-- tag_OA: string (nullable = true)
|-- tag_OC: string (nullable = true)
|-- tag_OP: integer (nullable = true)
|-- tag_OQ: string (nullable = true)
|-- tag_OX: string (nullable = true)
|-- tag_PG: string (nullable = true)
|-- tag_PQ: integer (nullable = true)
|-- tag_PT: string (nullable = true)
|-- tag_PU: string (nullable = true)
|-- tag_Q2: string (nullable = true)
|-- tag_QT: string (nullable = true)
|-- tag_QX: string (nullable = true)
|-- tag_R2: string (nullable = true)
|-- tag_RG: string (nullable = true)
|-- tag_RX: string (nullable = true)
|-- tag_SA: string (nullable = true)
|-- tag_SM: integer (nullable = true)
|-- tag_TC: integer (nullable = true)
|-- tag_U2: string (nullable = true)
|-- tag_UQ: integer (nullable = true)

>>> reads_df.select("qname", "contig", "pos_start", "pos_end", "cigar", "seq" ).show(5)
+--------------------+------+---------+-------+-----+--------------------+
|               qname|contig|pos_start|pos_end|cigar|                 seq|
+--------------------+------+---------+-------+-----+--------------------+
|61DC0AAXX100127:8...|    MT|        7|    107| 101M|AGGTCTATCACCCTATT...|
|61DC0AAXX100127:8...|    MT|        9|    109| 101M|GTCTGTCACCCTTGTAG...|
|61DC0AAXX100127:8...|    MT|       10|    110| 101M|TCTATCCCCCTATTAAC...|
|61DC0AAXX100127:8...|    MT|       20|    120| 101M|TATTATCCACTCACGGG...|
|61CC3AAXX100125:5...|    MT|       25|    100|  76M|ACCACTCACGGGAGCTC...|
+--------------------+------+---------+-------+-----+--------------------+
only showing top 5 rows

>>> reads_df.count()
22607

{{< /tab >}}
{{< /tabpane >}}
