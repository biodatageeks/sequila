package org.apache.spark.sql.hive.thriftserver.ui



import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2Seq.HiveThriftServer2ListenerSeq
import org.apache.spark.sql.hive.thriftserver.{HiveThriftServer2, SequilaThriftServer}
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab._
import org.apache.spark.ui.{SparkUI, SparkUITab}

/**
  * Spark Web UI tab that shows statistics of jobs running in the thrift server.
  * This assumes the given SparkContext has enabled its SparkUI.
  */
private[thriftserver] class ThriftServerTabSeq(sparkContext: SparkContext, list: HiveThriftServer2ListenerSeq)
  extends SparkUITab(getSparkUI(sparkContext), "sqlserver") with Logging {

  override val name = "SeQuiLa JDBC/ODBC Server"

  val parent = getSparkUI(sparkContext)
  val listener = list

  attachPage(new ThriftServerPageSeq(this))
  attachPage(new ThriftServerSessionPageSeq(this))
  parent.attachTab(this)

  def detach() {
    getSparkUI(sparkContext).detachTab(this)
  }
}

private[thriftserver] object ThriftServerTab {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}