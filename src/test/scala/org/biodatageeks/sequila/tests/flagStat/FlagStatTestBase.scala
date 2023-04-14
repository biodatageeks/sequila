package org.biodatageeks.sequila.tests.flagStat
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.sequila.utils.InternalParams
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import scala.reflect.io.Directory

class FlagStatTestBase extends FunSuite
  with DataFrameSuiteBase
  with BeforeAndAfter
  with SharedSparkContext{

  System.setSecurityManager(null)

  val sampleId = "NA12878.multichrom.md"
  val bamPath: String = getClass.getResource(s"/multichrom/mdbam/${sampleId}.bam").getPath;
  val tableName = "reads_bam"
  var ss : SequilaSession = null;

  def cleanup(dir: String) = {
    val directory = new Directory(new File(dir))
    directory.deleteRecursively()
  }

  val flagStatQuery =
    s"""
       |SELECT *
       |FROM  flagstat('$tableName', '${sampleId}')
    """.stripMargin

  before {
    spark.sqlContext.setConf(InternalParams.SerializationMode, StorageLevel.DISK_ONLY.toString());
    spark.conf.set("spark.sql.shuffle.partitions", 1);
    ss = SequilaSession(spark);
    ss.sparkContext.setLogLevel("ERROR");
    ss.sqlContext.setConf(InternalParams.BAMValidationStringency, "SILENT");
    ss.sqlContext.setConf(InternalParams.UseIntelGKL, "true");
    ss.sqlContext.setConf(InternalParams.IOReadAlignmentMethod, "hadoopBAM");
    ss.sql(s"DROP TABLE IF EXISTS $tableName");
    ss.sql(s"""
							|CREATE TABLE $tableName
							|USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
							|OPTIONS(path "$bamPath")
							|
     	""".stripMargin);
  }
}
