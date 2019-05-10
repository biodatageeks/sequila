Depth of coverage analyses
==========================


.. contents::

Operations
############

bdg_coverage - Calculate depth of coverage
-------------------------------------------

bdg_coverage is a table-valued function that calculates depth of coverage for specified sample. It operates on a table containg aligned reads. 

.. seealso::
   
   Have a look on our custom data source providing tabular view on BAM files :ref:`Data formats`


It can return results in blocks (which is the default, more efficient approach), with per base granularity or average coverage in fixed-length windows.


Below you can find end-to-end code snippet from BAM file to calculating coverage.

.. code-block:: scala

  
  val tableNameBAM = "reads"
  //
  // path to your BAM file 
  val bamPath = "file:///Users/aga/workplace/data/NA12878.chr21.bam"
  // create database DNA
  ss.sql("CREATE DATABASE dna")
  ss.sql("USE dna")

   // create table reads using BAM data source
   ss.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
    """.stripMargin)

  //calculate coverage - example for blocks coverage
  
  ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks')").show(5)
  
          +----------+-----+---+--------+
          |contigName|start|end|coverage|
          +----------+-----+---+--------+
          |      chr1|   34| 34|       1|
          |      chr1|   35| 35|       2|
          |      chr1|   36| 37|       3|
          |      chr1|   38| 40|       4|
          |      chr1|   41| 49|       5|
          +----------+-----+---+--------+
  
  
  //calculate coverage - example for per-base coverage
  
  ss.sql(s"SELECT contigName, start, coverage FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'bases')").show(5)
  
          +----------+-----+--------+
          |contigName|start|coverage|
          +----------+-----+--------+
          |      chr1|   34|       1|
          |      chr1|   35|       2|
          |      chr1|   36|       3|
          |      chr1|   37|       3|
          |      chr1|   38|       4|
          +----------+-----+--------+
  
  //calculate coverage - example for fixed-lenght windows coverage
  
  ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks', 100)").show(5)
          +----------+-----+---+--------+
          |contigName|start|end|coverage|
          +----------+-----+---+--------+
          |      chr1|    0| 99|6.030303|
          |      chr1|  200|299|    1.68|
          |      chr1|  500|599|    4.69|
          |      chr1|  100|199|    1.61|
          |      chr1|  400|499|    3.05|
          |      chr1|  300|399|    1.82|
          +----------+-----+---+--------+



Functional Parameters
######################
bdg_coverage function takes four parameters. First one is table name (with aligned reads), second one is sample identifier. The remaining two are described below.


result - choose coverage result type
-----------------------------------------
The result type determines the output of the algorithm. It can take two values: `blocks` or `bases`. This parameter is optional, and if absent blocks output is used by default as it is less verbose than bases option. 

.. code-block:: scala

   //calculate coverage - example for blocks coverage
  ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks')")
  //calculate coverage - example for bases coverage
  ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'bases')")


target - choose target
-------------------------
The ``target`` parameter is optional and if it used it means that fixed-lenght window result will be produced with the value of ``target`` being the length of the window. When this parameter is used, the value of resultType is ignored

.. code-block:: scala

   //calculate coverage - example for fixed-lenght windows coverage (length = 100)
  ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks', 100)")
  //calculate coverage - example for fixed-lenght windows coverage (length = 500)
  ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'bases', 500)")


Configuration Parameters
##########################

ShowAllPositions - determine output positions
-----------------------------------------------
This boolean configuration parameter determines whether all positions should be included in the generated in the output (when set to ``true``), or to skip regions with zero coverage (when set to ``false``). The default value is ``false``.

.. code-block:: scala

    //assuming that ss is the registered SeQuiLa session
    // this should be performed before calculating coverage - set to true
    ss.sqlContext.setConf(BDGInternalParams.ShowAllPositions,"true")
    ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks')")
	// parameter can be reverted back to false
    ss.sqlContext.setConf(BDGInternalParams.ShowAllPositions,"false")
    ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks')")



filterReadsByFlag - determine skipping reads condition
-------------------------------------------------------
This parameters is used to filter out reads used for coverage calculations. The algorithm discards all reads that have SAM flag containing any of the bits from ``filterReadsByFlag``  set. By default the value of ``filterReadsByFlag`` is 1796 and means filtering out reads that are  (unmapped (0x4), not primary alignment (0x100), fail platform/vendor quality checks (0x200) are PCR or optical duplicate (0x400))

.. code-block:: scala

    //assuming that ss is the registered SeQuiLa session
    // this should be performed before calculating coverage - set to desired filterflag
    ss.sqlContext.setConf(BDGInternalParams.filterReadsByFlag,1792)
    ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks')")
	// parameter can be reverted back to default
    ss.sqlContext.setConf(BDGInternalParams.filterReadsByFlag,1796)
    ss.sql(s"SELECT * FROM bdg_coverage('${tableNameBAM}','NA12878.chr21', 'blocks')")


InputSplitSize - determine partitions size 
-------------------------------------------
This parameter may be used if the user wants to manually control partitions size. It does not affect the coverage result, but it may affect the performance.

.. code-block:: scala

   // set the partitions size to 64 MB
   ss.sqlContext.setConf(BDGInternalParams.InputSplitSize, “67108864”)





