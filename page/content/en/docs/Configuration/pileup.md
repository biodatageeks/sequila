
---
title: "Pileup and coverage"
linkTitle: "Pileup and coverage"
weight: 2
description: >
    Pileup and coverage
---

| Property Name                           | Default         | Meaning                                                                            |
|-----------------------------------------|-----------------|------------------------------------------------------------------------------------|
|spark.biodatageeks.coverage.filterFlag | 1796 | Which SAM flags to use for filtering records. Check [SAM explainer](https://broadinstitute.github.io/picard/explain-flags.html) for details.|
|spark.biodatageeks.coverage.minMapQual| 0| Minimal value of reads' mapping quality to be included in coverage/pileup calculations |
|spark.biodatageeks.pileup.useVectorizedOrcWriter | false | Whether to use vectorized ORC writer and to bypass Spark's InternalRow serialization and write directly to the ouput file. This option is still experimental but speeds up saving results substantially.|
|spark.biodatageeks.pileup.maxBaseQuality | 40 | Specify the number of base quality values - internally used for allocating BQ counters. |

