
---
title: "BED"
linkTitle: "BED"
weight: 3
description: >
    SeQuiLa's BED format support
---

## Examples

```bash
mkdir -p /tmp/data
wget -nc https://github.com/biodatageeks/pysequila/raw/master/features/data/targets.bed -O /tmp/data/targets.bed
```


{{< tabpane >}}
{{< tab header="SQL" lang="sql" >}}
CREATE TABLE IF NOT EXISTS targets
USING org.biodatageeks.sequila.datasources.BED.BEDDataSource
OPTIONS(path "/tmp/data/targets.bed");

DESCRIBE TABLE targets;

+------------+----------+-------+
|    col_name| data_type|comment|
+------------+----------+-------+
|      contig|    string|   null|
|   pos_start|       int|   null|
|     pos_end|       int|   null|
|        name|    string|   null|
|       score|       int|   null|
|      strand|    string|   null|
| thick_start|       int|   null|
|   thick_end|       int|   null|
|    item_rgb|array<int>|   null|
| block_count|       int|   null|
| block_sizes|array<int>|   null|
|block_starts|array<int>|   null|
+------------+----------+-------+

SELECT * FROM targets;

+------+---------+-------+----+-----+------+-----------+---------+--------+-----------+-----------+------------+
|contig|pos_start|pos_end|name|score|strand|thick_start|thick_end|item_rgb|block_count|block_sizes|block_starts|
+------+---------+-------+----+-----+------+-----------+---------+--------+-----------+-----------+------------+
|    MT|       11|     50|null| null|  null|       null|     null|    null|       null|       null|        null|
|    MT|      201|    300|null| null|  null|       null|     null|    null|       null|       null|        null|
+------+---------+-------+----+-----+------+-----------+---------+--------+-----------+-----------+------------+

{{< /tab >}}
{{< tab header="Python" lang="python">}}
>>> from pysequila import SequilaSession
>>> ss = SequilaSession \
.builder \
.getOrCreate()

>>> targets_df = ss.read\
.format("org.biodatageeks.sequila.datasources.BED.BEDDataSource")\
.load("/tmp/data/targets.bed")

>>> targets_df.printSchema()
root
|-- contig: string (nullable = true)
|-- pos_start: integer (nullable = false)
|-- pos_end: integer (nullable = false)
|-- name: string (nullable = true)
|-- score: integer (nullable = true)
|-- strand: string (nullable = true)
|-- thick_start: integer (nullable = true)
|-- thick_end: integer (nullable = true)
|-- item_rgb: array (nullable = true)
|    |-- element: integer (containsNull = false)
|-- block_count: integer (nullable = true)
|-- block_sizes: array (nullable = true)
|    |-- element: integer (containsNull = false)
|-- block_starts: array (nullable = true)
|    |-- element: integer (containsNull = false)

>>> targets_df.show()
+------+---------+-------+----+-----+------+-----------+---------+--------+-----------+-----------+------------+
|contig|pos_start|pos_end|name|score|strand|thick_start|thick_end|item_rgb|block_count|block_sizes|block_starts|
+------+---------+-------+----+-----+------+-----------+---------+--------+-----------+-----------+------------+
|    MT|       11|     50|null| null|  null|       null|     null|    null|       null|       null|        null|
|    MT|      201|    300|null| null|  null|       null|     null|    null|       null|       null|        null|
+------+---------+-------+----+-----+------+-----------+---------+--------+-----------+-----------+------------+

{{< /tab >}}
{{< /tabpane >}}