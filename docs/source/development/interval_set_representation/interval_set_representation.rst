Interval Set representation
========


In the first version of SeQuiLa we used Interval red black tree data structure implemented in Java. 
On the other, we are aware that state-of-the-art non-distributed tools such as (bed-tk, cgranges, genomicRanges, etc) utilizes  data structures that are more efficient in finding overlaps between sets of intervals, i.e. Augmented Interval Lists, Nested Contained Lists, Implicit Interval Trees, etc.  

In the current version of SeQuiLa we provide the interface that allows to test new data structures and find overlaps algorithms. To replace the orginal implementation of RedBlack interval tree, the following steps need to be done: 

- Add a new class of IntervalTreeHolder that implements interface defined in BaseIntervalHolder.scala.

- Add a new class of Node that implements interface defined in BaseNode.scala

- Set InternalParams.intervalHolderClass parameter in spark.sqlContext configuration.

- Sample implementation of DummyIntervalHolder and its use can be found in CustomIntervalHolderTestSuits.
