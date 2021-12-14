
---
title: "SQL"
linkTitle: "SQL"
weight: 1
description: >
    SeQuiLa's SQL API
---

# Table of Contents

* [coverage](#sequila.SequilaSession.coverage)
* [pileup](#sequila.SequilaSession.pileup)


{{% alert title="Watchout" color="warning" %}}
Check [Configuration](../../configuration) page for other configuration options that applicable for all types of APIs.
{{% /alert %}}

<a id="sequila.SequilaSession.coverage"></a>
## coverage
```sql
 coverage(table_name , sample_name, ref_path) 
```

### Parameters


| Parameter Name                          | Type            | Meaning          |
|-----------------------------------------|-----------------|------------------|
|table_name | str | name of the external table created over alignment file(s) in BAM/CRAM format (with an index file - both BAI and SBI are supported)
|sample_name | str | name of the sample in `table_name` to use for coverage calculations
|ref_path | str | the reference file in FASTA format (with an index file)

### Examples

```sql
CREATE TABLE IF NOT EXISTS reads
USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
OPTIONS(path "{{ PWD }}/data/NA12878.bam");
-- calculate coverage
SELECT * FROM coverage('reads', 'NA12878','{{ PWD }}/data/hg18.fasta') LIMIT 10;

+------+---------+-------+---+--------+
|contig|pos_start|pos_end|ref|coverage|
+------+---------+-------+---+--------+
|     1|       34|     34|  R|       1|
|     1|       35|     35|  R|       2|
|     1|       36|     37|  R|       3|
|     1|       38|     40|  R|       4|
|     1|       41|     49|  R|       5|
|     1|       50|     67|  R|       6|
|     1|       68|    109|  R|       7|
|     1|      110|    110|  R|       6|
|     1|      111|    111|  R|       5|
|     1|      112|    113|  R|       4|
+------+---------+-------+---+--------+
only showing top 10 rows
```

<a id="sequila.SequilaSession.pileup"></a>
## coverage
```sql
 pileup(table_name , sample_name, ref_path, include_quals) 
```

### Parameters


| Parameter Name                          | Type            | Meaning          |
|-----------------------------------------|-----------------|------------------|
|table_name | str | name of the external table created over alignment file(s) in BAM/CRAM format (with an index file - both BAI and SBI are supported)
|sample_name | str | name of the sample in `table_name` to use for coverage calculations
|ref_path | str | the reference file in FASTA format (with an index file)
|include_quals|bool(false)| whether to include base qualities pileup as well for positions with alts |

### Examples

```sql
CREATE TABLE IF NOT EXISTS reads
USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
OPTIONS(path "{{ PWD }}/data/NA12878.bam");
-- calculate pileup
SELECT * FROM pileup('reads', 'NA12878','{{ PWD }}/data/hg18.fasta', false) LIMIT 5;

+------+---------+-------+---------+--------+--------+-----------+----+-----+
|contig|pos_start|pos_end|      ref|coverage|countRef|countNonRef|alts|quals|
+------+---------+-------+---------+--------+--------+-----------+----+-----+
|     1|       34|     34|        C|       1|       1|          0|null| null|
|     1|       35|     35|        C|       2|       2|          0|null| null|
|     1|       36|     37|       CT|       3|       3|          0|null| null|
|     1|       38|     40|      AAC|       4|       4|          0|null| null|
|     1|       41|     49|CCTAACCCT|       5|       5|          0|null| null|
+------+---------+-------+---------+--------+--------+-----------+----+-----+
only showing top 5 rows
```