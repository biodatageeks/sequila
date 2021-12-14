
---
title: "Python"
linkTitle: "Python"
weight: 1
description: >
    SeQuiLa's Python API
---


# Table of Contents

* [sequila](#sequila)
    * [SequilaSession](#sequila.SequilaSession)
        * [Builder](#sequila.SequilaSession.Builder)
        * [coverage](#sequila.SequilaSession.coverage)
        * [pileup](#sequila.SequilaSession.pileup)

<a id="sequila"></a>

{{% alert title="Watchout" color="warning" %}}
Check [Configuration](../../configuration) page for other configuration options that applicable for all types of APIs.
{{% /alert %}}

# sequila

Entrypoint to Sequila - tool for large-scale genomics on Spark.

<a id="sequila.SequilaSession"></a>

## SequilaSession Objects

```python
class SequilaSession(SparkSession)
```

Wrapper for SparkSession.

<a id="sequila.SequilaSession.Builder"></a>

## Builder Objects

```python
class Builder(SparkSession.Builder)
```

Builder for :class:`SequilaSession`.

<a id="sequila.SequilaSession.Builder.getOrCreate"></a>

#### getOrCreate

```python
def getOrCreate()
```

Get an existing :class:`SequilaSession`.

This is to add SequilaSession wrapper around SparkSession.

<a id="sequila.SequilaSession.__init__"></a>

### Examples

```python
from pysequila import SequilaSession
ss = SequilaSession \
.builder \
.getOrCreate()
```


<a id="sequila.SequilaSession.coverage"></a>
## coverage

```python
def coverage(path: str, refPath: str) -> DataFrame
```

Create a :class:`DataFrame` with depth of coverage for a specific aligment file.

.. versionadded:: 0.3.0

### Parameters

| Parameter Name                          | Type            | Meaning          |
|-----------------------------------------|-----------------|------------------|
path | str | the alignment file in BAM/CRAM format (with an index file)
|refPath | str | the reference file in FASTA format (with an index file)

### Returns

:class:`DataFrame`

### Examples

```bash
>>> ss.coverage(bam_file, ref_file).show(1)
+------+---------+-------+---+--------+
|contig|pos_start|pos_end|ref|coverage|
+------+---------+-------+---+--------+
|     1|       34|     34|  R|       1|
+------+---------+-------+---+--------+
only showing top 1 row
```
<a id="sequila.SequilaSession.pileup"></a>

## pileup

```python
def pileup(path: str, refPath: str, qual: bool) -> DataFrame
```

Create a :class:`DataFrame` with pileup for a specific aligment file.

.. versionadded:: 0.3.0

### Parameters

| Parameter Name                          | Type            | Meaning          |
|-----------------------------------------|-----------------|------------------|
|path | str |the alignment file in BAM/CRAM format (with an index file)
|refPath | str| the reference file in FASTA format (with an index file)
|qual | bool(True) | whether to include base qualities pileup in the output

### Returns

:class:`DataFrame`

### Examples

```bash
>>> ss.pileup(bam_file, ref_file, True).where("alts IS NOT NULL").show(1)
+------+---------+-------+---+--------+--------+-----------+---------+--------------------+
|contig|pos_start|pos_end|ref|coverage|countRef|countNonRef|     alts|               quals|
+------+---------+-------+---+--------+--------+-----------+---------+--------------------+
|     1|       69|     69|  A|       7|       6|          1|{99 -> 1}|{65 -> [0, 0, 0, ...|
+------+---------+-------+---+--------+--------+-----------+---------+--------------------+
only showing top 1 row
```