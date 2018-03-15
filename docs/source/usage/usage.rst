Usage
=====

Ad-hoc analysis
#################

For ad-hoc analysis SeQuiLa provides two usage patterns:

using predefined scripts in docker container
**********************************************

In SeQuiLa's docker image are two predefined scripts written to be executable from commandline in good-old fashion.  No need of Scala and Spark is needed.


writing short analysis in bdg-shell
************************************

For Scala enthusiasts - SeQuiLa provides bdg-shell which is a wrapper for spark-shell. It has extra strategy registered  and configuration already set, so it is fit for quick analysis.

<TODO> example



Integration with Spark application
####################################

When you have exisiting analysis pipeline in Spark ecosystem you may beenfit from SeQuiLa extra strategy registered at SparkSQL level.

<TODO> opis krokow


Running as Spark Thrift Server
##############################
fsdfsd