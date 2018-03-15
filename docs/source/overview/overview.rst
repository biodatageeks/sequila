Overview
========


Motivation 
##########

Motivation for implementing SeQuiLa was providing both elastic, fast and most importantly scalable solution for performing range join queries on genomic datasets. Range joins are bread and butter for NGS analysis but high volume of data make them very slow or even failing to compute. 

SeQuiLa is elastic - since it's SQL queries that are run upon datasets, its fast - due to efficient IntervalTree algorithm usage with additional optimizations and it's scalable - processing time decreases when run on Spark cluster.

Solution overview
#################

SeQuiLa is running upon Apache Spark execution engine. At high level it allows data analyst to write custom SQL queries that involve range joins on two datasets. Additional strategy injected into Spark guarantees good performance level.

.. image:: sequila.*


Methods and algorithms (Implementation)
########################################


