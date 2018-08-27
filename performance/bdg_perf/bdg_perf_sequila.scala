import org.apache.spark.sql.SequilaSession
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
import org.biodatageeks.BDGPerf
import org.biodatageeks.BDGPerf.{BDGPerfRunner, BDGQuery, BDGTestParams}

/*set params*/

val ss = SequilaSession(spark)

/*register UDFs*/

UDFRegister.register(ss)

/*inject strategies and data sources*/
SequilaRegister.register(ss)



val BAM_DIR = s"${sys.env("BGD_PERF_SEQ_BAM_DIR")}/*.bam"

//preparation
ss.sql(s"""
CREATE TABLE IF NOT EXISTS reads
USING org.biodatageeks.datasources.BAM.BAMDataSource
OPTIONS(path '${BAM_DIR}')""")

//targets
val  bedPath="/data/granges/tgp_exome_hg18.bed"
ss.sql(s"""
             |CREATE TABLE IF NOT EXISTS targets(contigName String,start Integer,end Integer)
             |USING csv
             |OPTIONS (path "${bedPath}", delimiter "\t")""".stripMargin)

ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","true")

val queries = Array(
  BDGQuery("bdg_seq_count_NA12878","SELECT COUNT(*) FROM reads WHERE sampleId='NA12878'"),
  BDGQuery("bdg_seq_filter_NA12878","SELECT COUNT(*) FROM reads WHERE sampleId='NA12878' and contigName='chr8' AND start>100000 AND end<110000"),
  BDGQuery("bdg_seq_int_join_NA12878",
    """
      |SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
      |     ON (targets.contigName=reads.contigName
      |     AND
      |     CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
      |     AND
      |     CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
      |     )
      |     GROUP BY targets.contigName,targets.start,targets.end
    """.stripMargin)
)

BDGPerfRunner.run(ss,queries)


//cleanup
ss.sql(s"""DROP TABLE IF EXISTS reads""")
ss.sql(s"""DROP TABLE IF EXISTS targets""")
System.exit(0)


