 .. sectnum::
     :start: 3

Usage
=====



Ad-hoc analysis
#################

For ad-hoc analysis SeQuiLa provides two usage patterns:

Using predefined scripts in docker container
**********************************************

In SeQuiLa's docker image are two predefined scripts written to be executable from commandline in good-old fashion.  No need of Scala and Spark is needed.

   |

.. figure:: docker.*

   Sample usage for findOverlaps


Writing short analysis in bdg-shell
************************************

For Scala enthusiasts - SeQuiLa provides bdg-shell which is a wrapper for spark-shell. It has extra strategy registered  and configuration already set, so it is fit for quick analysis.

   |

.. figure:: bdg-shell.*

   Sample ad-hoc analysis



<TODO> example

------------

Integration with existing applications
#######################################



Integration with Spark-application
***********************************
When you have exisiting analysis pipeline in Spark ecosystem you may beenfit from SeQuiLa extra strategy registered at SparkSQL level.


.. figure:: spark-integration.* 
   :align: center

<TODO> opis krokow



Integration with non Spark-application
***************************************

.. figure:: thrift-server.* 
   :align: center

