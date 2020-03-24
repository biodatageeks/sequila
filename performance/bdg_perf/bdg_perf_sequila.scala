import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.{Columns, SequilaRegister, UDFRegister}
import org.biodatageeks.BDGPerf
import org.biodatageeks.BDGPerf.{BDGPerfRunner, BDGQuery, BDGTestParams}

/*set params*/

val ss = SequilaSession(spark)

/*register UDFs*/

UDFRegister.register(ss)

/*inject strategies and data sources*/
SequilaRegister.register(ss)



val BAM_DIR = s"${sys.env("BGD_PERF_SEQ_BAM_DIR")}/*.bam"
val CRAM_DIR = s"${sys.env("BGD_PERF_SEQ_BAM_DIR")}/*.cram"
val FASTA_PATH = s"${sys.env("BGD_PERF_SEQ_BAM_DIR")}/Homo_sapiens_assembly18.fasta"
val bamTable = "reads_bam"
val cramTable = "reads_cram"

//preparation
ss.sql(s"""
CREATE TABLE IF NOT EXISTS ${bamTable}
USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
OPTIONS(path '${BAM_DIR}')""")

ss.sql(s"""
CREATE TABLE IF NOT EXISTS ${cramTable}
USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
OPTIONS(path '${CRAM_DIR}')""")

//targets
val  bedPath="/data/granges/tgp_exome_hg18.bed"
ss.sql(s"""
             |CREATE TABLE IF NOT EXISTS targets(${Columns.CONTIG} String,${Columns.START} Integer,${Columns.END} Integer)
             |USING csv
             |OPTIONS (path "${bedPath}", delimiter "\t")""".stripMargin)

ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","true")

val queries = Array(
  BDGQuery("bdg_seq_count_NA12878",s"SELECT COUNT(*) FROM ${bamTable} WHERE ${Columns.SAMPLE}='NA12878'"),
  BDGQuery("bdg_seq_filter_NA12878",s"SELECT COUNT(*) FROM ${bamTable} WHERE ${Columns.SAMPLE}='NA12878' and ${Columns.CONTIG}='8' AND ${Columns.START}>100000 AND ${Columns.END}<110000"),
  BDGQuery("bdg_cov_count_NA12878_BAM",s"SELECT COUNT(*) FROM bdg_coverage ('${bamTable}','NA12878', 'blocks')"),
  BDGQuery("bdg_cov_count_NA12878_CRAM",s"SELECT COUNT(*) FROM bdg_coverage ('${cramTable}','NA12878', 'blocks')"),
  BDGQuery("bdg_seq_int_join_NA12878",
    s"""
      |SELECT targets.${Columns.CONTIG},targets.${Columns.START},targets.${Columns.END},count(*) FROM ${bamTable} JOIN targets
      |     ON (targets.${Columns.CONTIG}=${bamTable}.${Columns.CONTIG}
      |     AND
      |     CAST(${bamTable}.${Columns.END} AS INTEGER)>=CAST(targets.${Columns.START} AS INTEGER)
      |     AND
      |     CAST(${bamTable}.${Columns.START} AS INTEGER)<=CAST(targets.${Columns.END} AS INTEGER)
      |     )
      |     GROUP BY targets.${Columns.CONTIG},targets.${Columns.START},targets.${Columns.END}
    """.stripMargin),
  BDGQuery("bdg_cov_window_fix_length_500_count_NA12878_BAM",s"SELECT COUNT(*) FROM bdg_coverage ('${bamTable}','NA12878', 'bases','500')"),
  BDGQuery("bdg_cov_window_fix_length_500_count_NA12878_CRAM",s"SELECT COUNT(*) FROM bdg_coverage ('${cramTable}','NA12878', 'bases','500')"),
  BDGQuery("bdg_cov_bases_NA12878_BAM",s"SELECT COUNT(*) FROM bdg_coverage ('${bamTable}','NA12878', 'bases')"),
  BDGQuery("bdg_pileup_cov_only_NA12878",s"SELECT COUNT(*) FROM pileup ('${bamTable}')")

)

BDGPerfRunner.run(ss,queries)


//cleanup
ss.sql(s"""DROP TABLE IF EXISTS ${bamTable}""")
ss.sql(s"""DROP TABLE IF EXISTS ${cramTable}""")

ss.sql(s"""DROP TABLE IF EXISTS targets""")
System.exit(0)


