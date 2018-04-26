

Overview
========

Motivation 
##########

Motivation for implementing SeQuiLa was providing both elastic, fast and most importantly scalable solution for performing range join queries on genomic datasets. Range joins are bread and butter for NGS analysis but high volume of data make them very slow or even failing to compute. 

SeQuiLa is elastic - since it's SQL queries that are run upon datasets, it's fast - thanks to efficient IntervalTree algorithm
usage with additional optimizations and it's scalable - processing time decreases when run on Spark cluster.

Solution overview
#################

SeQuiLa is running upon Apache Spark execution engine.
At high level it allows data analyst to write custom SQL queries that involve range joins on two datasets.
Additional strategy injected into Spark guarantees good performance level.

.. image:: sequila.*

But hey, you don’t need to know Spark or Scala to use SeQuiLa.

SeQuiLa can be easily integrated in your existing processing pipeline.
And this is it's essential feature. Whether you prefer to do your analysis via commandline tools or from spark-shell or only inject efficient range joins into your exisiting Spark or non-Spark application - SeQuiLa is there for you.

For details see: :doc:`../usage/usage`



Algorithm
###########

SeQuiLa's range joins are based on IntervalTree algorithm. 
SeQuiLa package at its core introduces a new rule based optimizer (RBO) that chooses most efficient join strategy based on
input data statistics computed in the runtime. The first step of the algorithm is to obtain value of `spark.biodatageeks.rangejoin.maxBroadcastSize` parameter. It can set explicite by the end user or computed as a fraction of the Apache Spark Driver memory.
In the next step table row counts are computed and based on that table with the fewer rows is selected for constructing interval forest. This is the default approach - it can be overridden by setting
`spark.biodatageeks.rangejoin.useJoinOrder` to `true`. In this scenario no row counts are computed and the right join table is used for creating interval forest. Such an strategy can be useful in situation when it is known upfront which table should be used for creating a broadcast structure. The final step of the optimization procedure is to estimate the row size and the size of the whole table.
If this number is lower than the max broadcast size parameter computed above then complete table rows are put in the interval tree broadcast structure otherwise only intervals identifiers are used.

.. image:: rbo.*


Tests
######

Accuracy
*********

SeQuiLa's results were tested for compatibility with GRganges R package. It achieved 100% accuracy rate.


Performance tests
******************

During performance testing phase we focused on similar tools as well as compared our strategy against default
Apache Spark strategy used for genomic interval queries. SeQuiLa clearly outperforms all the competing tools:

.. image:: local_cluster_speedup.*

Repositories
#############

You can find SeQuiLa publicly available in following repositories:


==========   =====================================================================  
Repo         Link
==========   =====================================================================
GitHub       `<https://github.com/ZSI-Bio/|project_name|>`_
Maven        `<https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/>`_ 
Docker Hub   `<https://hub.docker.com/r/biodatageeks/|project_name|/>`_
==========   ===================================================================== 