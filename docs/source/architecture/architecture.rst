Methods
========


SeQuiLa ecosystem
###################

SeQuiLa is a distributed, ANSI-SQL compliant solution for efficient genomic operations (currently available genomic intervals querying/processing and depth of coverage calculations) that is available as an additional Apache Spark package. 

In runtime it extends SparkSQL Catalyst optimizer with custom execution strategies. It implements distributed map joins using interval forest for inner range join operations and distributed event-based algorithm for depth of coverage computations. Useful genomic transformations have been added as User Defined Functions/Aggregates and exposed to the SQL interface. Furthermore, SeQuiLa data sources for both BAM and ADAM file formats have been implemented.

It can be integrated with third-party tools using SparkSQL JDBC driver and with R using sparklyr package. SeQuiLa is also available as a Docker container and can be run locally or on a Hadoop cluster using Yet Another Resource Negotiator

.. figure:: components.*
    :scale: 100

    SeQuiLa's ecosystem


Distributed interval joins
##########################

Overview
---------

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



Algorithm
-----------

At it's core SeQuiLa's range joins are based on IntervalTree data structure. The main idea of the algorithm is to transform dataset `s1` into a broadcastable structure of an interval forest (a hash map of interval trees, each representing one chromosome). The intervals from dataset `s2` can be efficiently intersected with the constructed interval forest.

An interval tree is a tree data structure to hold intervals. It is a augmented, balanced red-black tree with low endpoint as node key and additional max value of any endpoint stored in subtree. 
Each node contains following fields: parent, left subtree, right subtree, color, low endpoint, high endpoint and max endpoint of subtree. 
It can be proved that this structure allows for correct interval insertion, deletion and search in :math:`O(lg n)` time ([CLR]_)

.. figure:: inttree.*
	:scale: 65

	An interval tree. On the top: A set of 10 intervals, shown sorted bottom to top by left endpoint. On  the bottom the interval tree that represents them. An inorder tree walk of the tree lists the nodes in sorted order by left endpoint. [CLR]_

Our implementation of IntervalTree is based on explanations in [CLR]_ although it is extended in the following ways:

* data structure allows storing non-unique intervals 
* data structure allows storing in the tree nodes additional interval annotations if they fit into dedicated Spark driver's memory fraction

Let's presume that we have a cluster with a Spark driver, three worker nodes and that the tables are partitioned between worker nodes. When interval query is performed, all dataset's :math:`s1` partitions are sent to the driver node on which interval forest is constructed (for each chromosome a separate interval tree is created).  The forest is subsequently sent back to worker nodes on which efficient interval operations based on interval trees are performed. Depending on the strategy chosen by rule-based optimizer different set of columns are stored in tree nodes


.. figure:: broadcast.*
    :scale: 80

    Broadcasting interval forest to worker nodes.




.. [CLR] Cormen, Thomas H.; Leiserson, Charles E., Rivest, Ronald L. (1990). Introduction to Algorithms (1st ed.). MIT Press and McGraw-Hill. ISBN 0-262-03141-8



Optimizations
---------------

SeQuiLa package introduces a new rule based optimizer (RBO) that chooses most efficient join strategy based on
input data statistics computed in the runtime. The first step of the algorithm is to obtain value of `maxBroadcastSize` parameter. It can set explicitly by the end user or computed as a fraction of the Apache Spark Driver memory.
In the next step table row counts are computed and based on that table with the fewer rows is selected for constructing interval forest. This is the default approach - it can be overridden by setting
`spark.biodatageeks.rangejoin.useJoinOrder` to `true`. In this scenario no row counts are computed and the right join table is used for creating interval forest. Such an strategy can be useful in situation when it is known upfront which table should be used for creating a broadcast structure. 
The final step of the optimization procedure is to estimate the row size and the size of the whole projected table.
If it fits into dedicated Spark Driver's memory (controlled by maxBroadcastSize parameter) the interval forest is augmented with all columns from s1 (SeQuiLa_it_all strategy) completing map-side join procedure in one stage. Otherwise an interval tree is used as an index for additional lookup step before the equi-shuffle-join operation between s1 and s2 (SeQuiLa_it_int strategy).


.. figure:: rbo.*
    :scale: 100

    Rule-based optimizer's algorithm chooses the most efficient join strategy.


Distributed depth of coverage
#############################

Overview
----------

Consider a set of aligned reads from a BAM file (read_set) for which we want to compute the depth of coverage.

Firstly, we provide a tabular view on the input data, through custom SQL-like data source. The data from the file can be queried using standard SELECT statements. Secondly, using our table-valued function, coverage for read_set can be calculated. 


.. code-block:: SQL

    -- create table read_set
    CREATE TABLE read_set 
    USING org.biodatageeks.org.biodatageeks.datasources.BAM.BAMDataSource 
    OPTIONS (path '/data/samples/*.bam'); 

    -- sample SQL query on read_set
    SELECT sample, contig, start, end, cigar, flag, sequence
    FROM read_set
    WHERE sample='sample1' AND flag=99 AND contig='chr3'

    -- calculate coverage on read_set
    SELECT contig, start, end, coverage
    FROM bdg_coverage('read_set','sample1','blocks')
    WHERE contig='chr3'




Algorithm
-----------

SeQuiLa-cov implements distributed event-based algorithm for coverage calculations. Event-based approach has better computational complexity than traditionally used pileup-based methods since it tracks only selected events of alignment blocks instead of analyzing each base of each alignment block. 

The event-based algorithm allocates ``events`` vector for each contig and subsequently iterates through the set of reads parsing it's CIGAR string to determine continuous alignment blocks. For each block the value in the ``events`` vector in the position corresponding to block start is being incremented and the value in the position corresponding to block end is being decremented.  The depth of coverage for a specific locus is calculated using the cumulative sum of all elements in the ``events`` vector preceding specified position. 

The algorithm may produce three typically used coverage types:  `per-base` coverage, which includes the coverage value for each genomic position separately, `blocks` which lists adjacent positions with equal coverage values are merged into single interval, and `fixed-length windows` coverage that generates set of equal-size, non-overlapping and tiling genomic ranges and outputs arithmetic mean of base coverage values for each region.


.. figure:: events.*
    :scale: 90

    
    Event-based approach and three coverage result types.



In the most general case, the algorithm can be used in a distributed environment. Let's consider input data set, ``read_set``, of aligned sequencing reads sorted by genomic position from a BAM file partitioned into the `n` data slices (``read_set_1``, ``read__set_2``, ..., ``read_set_n``).

In this setting each cluster node computes the coverage for the subset of data slices using the event-based method. For the i-th partition containing the set of reads (``read_set_i``), the set of  ``events_i`` vectors for each chromosome is allocated and updated, based on the items from ``read_set_i``. After performing the CIGAR analysis and appropriate updates on the ``events`` vectors, the cumulative sum is realized and stored in ``partial_coverage`` vectors for each chromosome.

Note that the set of ``partial_coverage_i`` vectors is distributed among the computation nodes. To calculate the final coverage for the input data set, an additional step of correction for overlaps between the partitions is required. 

An overlap of length `l` between vectors adjacent ``partial_coverage`` vectors may occur when `l` tailing genomic positions of preceding vector are the same as `l` heading genomic positions of consecutive vector.

If an overlap is identified then the coverage values from the preceding ``partial_coverage`` ``l``-length tail are added into the consecutive ``partial_coverage`` head and subsequently the last ``l`` elements of first ``partial_coverage`` are removed. Once this correction step is completed, non-overlapping set of ``coverage`` vectors are collected and yield the final coverage values for the whole input ``read_set``. 



.. figure:: coverage_algorithm.*
    :scale: 90

    Distributed event-based algorithm.


.. note::

   The described above algorithm is simplified. In the more general case (specifically when dealing with long reads), overlaps may occur between any number of partitions (more than two) and the overlap length can be bigger than partition length. 


Implementation and optimizations
----------------------------------

The main characteristic of the described above algorithm is its ability to distribute data and calculations (such as BAM decompression and main coverage procedure) among the available computation nodes. Moreover, instead of simply performing full data reduction stage of the partial coverage vectors, our solution minimizes required data shuffling  among cluster nodes by limiting it to the overlapping part of coverage vectors.

To efficiently access the data from a BAM file we have prepared a custom data source using Data Source API exposed by SparkSQL. Performance of the read operation benefits from the Intel Genomics Kernel Library  (`GKL <https://github.com/Intel-HLS/GKL>`_) used for decompressing the BAM files chunks and from predicate push-down mechanism that filters out data at the earliest stage.  

The implementation of the core coverage calculation algorithm aimed at minimizing, whenever possible memory footprint by using parsimonious data types, e.g. ``Short`` type instead of ``Integer``, and efficient memory allocation strategy for large data structures, e.g. favoring static ``Arrays`` over dynamic size ``ArrayBuffers``. 

Additionally, to reduce the overhead of data shuffling between the worker nodes in the correction for the overlaps stage we used Spark's shared variables accumulators and broadcast variables. Accumulator is used to gather information about the worker nodes' coverage vector ranges and coverage vector tail values, that are subsequently read and processed by the driver. This information is then used to construct a broadcast variable distributed to the worker nodes in order to perform adequate trimming and summing operations on partial coverage vectors.

SeQuiLa-cov computation model supports fine-grained parallelism at user-defined partition size in contrary to the traditional, coarse-grained parallelization strategies that involve splitting input data at a contig level. 


.. figure:: coverage_implementation.*
    :scale: 90

    Implementation of distributed event-based algorithm in Apache Spark framework.


.. Usage patterns
.. ###############

.. SeQuiLa can be used in different ways. Specifically, it supports ad-hoc research, which is typically focused on quick analysis on data stored in files. Depending on your preferences, you can use predefined scripts (findOverlaps and featureCounts) or write your own code snippets within spark-shell or our bdg-shell. Additionally SeQuiLa also can be used along with your existing applications written in Scala/Spark, R or any other language/platform.


.. .. figure:: usage_all.*
..     :scale: 100

..     SeQuiLa supports both file-oriented and data-oriented approach for analysis. Custom analysis can be written in SQL, R or Scala.


