
package org.apache.spark.sql.hive.thriftserver


import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.HiveThriftServer2Listener
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2Seq.HiveThriftServer2ListenerSeq
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.sql.{SQLContext, SequilaSession, SparkSession}
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab



object SequilaThriftServer extends Logging {
  var uiTab: Option[ThriftServerTab] = None
  var listener: HiveThriftServer2ListenerSeq = _

  def startWithContext(ss: SequilaSession): Unit = {
    val server = new HiveThriftServer2Seq(ss)

    val executionHive = HiveUtils.newClientForExecution(
      ss.sqlContext.sparkContext.conf,
      ss.sqlContext.sessionState.newHadoopConf())

    server.init(executionHive.conf)
    server.start()
    listener = new HiveThriftServer2ListenerSeq(server, ss.sqlContext.conf)
    ss.sqlContext.sparkContext.addSparkListener(listener)
    uiTab = if (ss.sqlContext.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
      Some(new ThriftServerTab(ss.sqlContext.sparkContext))
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    System.setSecurityManager(null)

    val spark = SparkSession
      .builder
      // .master("local[1]")
      .getOrCreate
    val ss = new SequilaSession(spark)
    UDFRegister.register(ss)
    SequilaRegister.register(ss)


    HiveThriftServer2Seq.startWithContext(ss)
  }

}

