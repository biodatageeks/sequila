Architecture
===============

Overview
#########


SeQuiLa is a distributed, ANSI-SQL compliant solution for efficient genomic intervals querying and processing that is available as an additional Apache Spark package. In the runtime it extends Apache SparkSQL Catalyst optimizer with custom execution strategies utilizing distributed interval forests based map joins for inner join operations using range conditions. 

Consider datasets `s1` and `s2`, storing genomic intervals, such as `|s1|<|s2|`. Let's assume that their structure contains necessary genomic coordinates (`chromosome`, `start position`, `end position`) along with optional additional interval annotations (e.g. targetId).

.. figure:: structure.*
    :scale: 100

    Dataset `s1` and `s2`. Both storing genomic intervals with necessary genomic coordinates and optional annotations

Our goal is to efficiently perform query as shown below:

.. code-block:: SQL

    SELECT s2.targetId,count(*)
    FROM reads s1 JOIN targets s2
    ON s1.chr=s2.chr
    AND s1.end>=s2.start
    AND s1.start<=s2.end
    GROUP BY targetId;

The main idea of the algorithm is to transform dataset `s1` into a broadcastable structure of an interval forest (a hash map of interval trees, each representing one chromosome). The intervals from dataset `s2` can be efficiently intersected with the constructed interval forest.


Let's additionally presume that we have a cluster with a Spark driver, three worker nodes and that the tables are partitioned between worker nodes.


.. figure:: broadcast.*
	:scale: 80

	Broadcasting interval forest to worker nodes.

When interval query is performed, all dataset's :math:`s1` partitions are sent to the driver node on which interval forest is constructed (for each chromosome a separate interval tree is created).  The forest is subsequently sent back to worker nodes on which efficient interval operations based on interval trees are performed. Depending on the strategy chosen by rule-based optimizer different set of columns are stored in tree nodes


Interval Tree
##############
At it's core SeQuiLa's range joins are based on IntervalTree data structure. 

An interval tree is a tree data structure to hold intervals. It is a augmented, balanced red-black tree with low endpoint as node key and additional max value of any endpoint stored in subtree. 
Each node contains following fields: parent, left subtree, right subtree, color, low endpoint, high endpoint and max endpoint of subtree. 
It can be proved that this structure allows for correct interval insertion, deletion and search in :math:`O(lg n)` time ([CLR]_)

.. figure:: inttree.*
	:scale: 65

	An interval tree. On the top: A set of 10 intervals, shown sorted bottom to top by left endpoint. On  the bottom the interval tree that represents them. An inorder tree walk of the tree lists the nodes in sorted order by left endpoint. [CLR]_

Our implementation of IntervalTree is based on explanations in [CLR]_ although it is extended in the following ways:

* data structure allows storing non-unique intervals 
* data structure allows storing in the tree nodes additional interval annotations if they fit into dedicated Spark driver's memory fraction


.. [CLR] Cormen, Thomas H.; Leiserson, Charles E., Rivest, Ronald L. (1990). Introduction to Algorithms (1st ed.). MIT Press and McGraw-Hill. ISBN 0-262-03141-8



Rule Based Optimizer
####################

SeQuiLa package introduces a new rule based optimizer (RBO) that chooses most efficient join strategy based on
input data statistics computed in the runtime. The first step of the algorithm is to obtain value of `maxBroadcastSize` parameter. It can set explicitly by the end user or computed as a fraction of the Apache Spark Driver memory.
In the next step table row counts are computed and based on that table with the fewer rows is selected for constructing interval forest. This is the default approach - it can be overridden by setting
`spark.biodatageeks.rangejoin.useJoinOrder` to `true`. In this scenario no row counts are computed and the right join table is used for creating interval forest. Such an strategy can be useful in situation when it is known upfront which table should be used for creating a broadcast structure. 
The final step of the optimization procedure is to estimate the row size and the size of the whole projected table.
If it fits into dedicated Spark Driver's memory (controlled by maxBroadcastSize parameter) the interval forest is augmented with all columns from s1 (SeQuiLa_it_all strategy) completing map-side join procedure in one stage. Otherwise an interval tree is used as an index for additional lookup step before the equi-shuffle-join operation between s1 and s2 (SeQuiLa_it_int strategy).


.. figure:: rbo.*
    :scale: 100

    Rule-based optimizer's algorithm chooses the most efficient join strategy.





SeQuiLa's ecosystem
###################

SeQuiLa has been developed in Scala using Apache Spark 2.2 environment. In runtime it extends SparkSQL Catalyst optimizer with custom execution strategies. It implements distributed map joins using interval forest for inner range join operations. Useful genomic transformations have been added as User Defined Functions/Aggregates and exposed to the SQL interface. Furthermore, SeQuiLa data sources for both BAM and ADAM file formats have been implemented.
It can be integrated with third-party tools using SparkSQL JDBC driver and with R using sparklyr package. SeQuiLa is also available as a Docker container and can be run locally or on a Hadoop cluster using Yet Another Resource Negotiator

.. figure:: components.*
    :scale: 100

    SeQuiLa's ecosystem


Usage patterns
###############

SeQuiLa can be used in different ways. Specifically, it supports ad-hoc research, which is typically focused on quick analysis on data stored in files. Depending on your preferences, you can use predefined scripts (findOverlaps and featureCounts) or write your own code snippets within spark-shell or our bdg-shell. Additionally SeQuiLa also can be used along with your existing applications written in Scala/Spark, R or any other language/platform.


.. figure:: usage_all.*
    :scale: 100

    SeQuiLa supports both file-oriented and data-oriented approach for analysis. Custom analysis can be written in SQL, R or Scala.


