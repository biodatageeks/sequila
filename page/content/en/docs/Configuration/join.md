---
title: "Interval joins"
linkTitle: "Interval joins"
weight: 3
description: >
    Interval joins
---

| Property Name                           | Default         | Meaning                                                                            |
|-----------------------------------------|-----------------|------------------------------------------------------------------------------------|
|spark.biodatageeks.rangejoin.useJoinOrder| false | Whether to always broadcast the right table of a join or to computer row counts and pick up the smaller one. |
|spark.biodatageeks.rangejoin.maxBroadcastSize| 0.1*spark.driver.memory| The maximum allowed size of the broadcasted intverval structure which is used by the SeQuiLa's optimizer to chose interval join [algorithm](http://biodatageeks.ii.pw.edu.pl/sequila/architecture/architecture.html#optimizations).|
|spark.biodatageeks.rangejoin.maxGap| 0 | The maximum gap between between regions |
|spark.biodatageeks.rangejoin.minOverlap| 1 | The minimal length of the overlap between regions |
|spark.biodatageeks.rangejoin.intervalHolderClass|[`IntervalTreeRedBlack`](https://github.com/biodatageeks/sequila/blob/master/src/main/scala/org/biodatageeks/sequila/rangejoins/methods/IntervalTree/IntervalTreeRedBlack.java)| [Pluggable](http://biodatageeks.ii.pw.edu.pl/sequila/architecture/architecture.html#custom-interval-structure) mechanism for implementing custom interval structures.|