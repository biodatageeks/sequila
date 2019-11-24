
package org.apache.spark.sql.hive.thriftserver


import java.io.File

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.HiveThriftServer2Listener
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2Seq.HiveThriftServer2ListenerSeq
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.sql.{SQLContext, SequilaSession, SparkSession}
import org.biodatageeks.sequila.utils.{SequilaRegister, UDFRegister}
import org.apache.spark.sql.hive.thriftserver.ui.{ThriftServerTab, ThriftServerTabSeq}



object SequilaThriftServer extends Logging {
  var uiTab: Option[ThriftServerTabSeq] = None
  var listener: HiveThriftServer2ListenerSeq = _

  @DeveloperApi
  def startWithContext(ss: SequilaSession): Unit = {
    //System.setSecurityManager(null)
    val server = new HiveThriftServer2Seq(ss)

    val executionHive = HiveUtils.newClientForExecution(
      ss.sqlContext.sparkContext.conf,
      ss.sparkContext.hadoopConfiguration)

    server.init(executionHive.conf)
    server.start()
    listener = new HiveThriftServer2ListenerSeq(server, ss.sqlContext.conf)
    ss.sqlContext.sparkContext.addSparkListener(listener)
    uiTab = if (ss.sqlContext.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
      Some(new ThriftServerTabSeq(ss.sqlContext.sparkContext,listener))
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    //System.setSecurityManager(null)
    val spark = SparkSession
      .builder
        .config("spark.sql.hive.thriftServer.singleSession","true")
        .config("spark.sql.warehouse.dir",sys.env.getOrElse("SEQ_METASTORE_LOCATION",System.getProperty("user.dir")) )
//        .config("spark.hadoop.hive.metastore.uris","thrift://localhost:9083")
      .enableHiveSupport()
//       .master("local[1]")
      .getOrCreate
    val ss = new SequilaSession(spark)
    UDFRegister.register(ss)
    SequilaRegister.register(ss)


    HiveThriftServer2Seq.startWithContext(ss)
  }

}

