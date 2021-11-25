import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.{SequilaSession, UDFRegister}


val ss = SequilaSession(spark)
UDFRegister.register(ss)

/*inject bdg-granges strategy*/
SequilaSession.register(ss)

ss.sqlContext.setConf("spark.sql.warehouse.dir","/data/output")

ss.sql(s"CREATE DATABASE IF NOT EXISTS sequila")
ss.sql(
  """
    |CREATE TABLE IF NOT EXISTS sequila.reads
    |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
    |OPTIONS(path "/data/input/bams/*.bam")
  """.stripMargin)

System.exit(0)

