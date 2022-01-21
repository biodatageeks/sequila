package org.biodatageeks.sequila.flagStat

import org.apache.spark.sql.{DataFrame, SequilaSession, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.sequila.utils.InternalParams

object FlagStatDebuggerEntryPoint {
	val bamFilePath: String = "D:\\\\NA12878.multichrom.bam";

	def main(args: Array[String]): Unit = {
		//execute(bamFilePath).show();
		executeSQL(bamFilePath, null).show();
	}

	def execute(bamPath: String): DataFrame = {
		System.setSecurityManager(null);
		val spark = SparkSession
			.builder()
			.master("local[1]")
			.config("spark.driver.memory","16g")
			.config("spark.sql.shuffle.partitions", 1)
			.config("spark.biodatageeks.bam.validation", "SILENT")
			.config("spark.biodatageeks.readAligment.method", "hadoopBAM")
			.config("spark.biodatageeks.bam.useGKLInflate", "true")
			.getOrCreate();
		spark.sqlContext.setConf(InternalParams.SerializationMode, StorageLevel.DISK_ONLY.toString())
		val fs = FlagStat(spark);
		fs.processFile(bamPath);
	}

	def executeSQL(tableNameOrPath: String, sampleId: String): DataFrame = {
		System.setSecurityManager(null);
		val spark = SparkSession
			.builder()
			.master("local[1]")
			.config("spark.driver.memory","16g")
			.config("spark.sql.shuffle.partitions", 1)
			.config("spark.driver.memory","16g")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			.config("spark.driver.maxResultSize","5g")
			.config("spark.ui.showConsoleProgress", "true")
			.config("spark.sql.catalogImplementation","in-memory")
			.getOrCreate();
		spark.sqlContext.setConf(InternalParams.SerializationMode, StorageLevel.DISK_ONLY.toString());
		var ss = SequilaSession(spark);
		ss.sqlContext.setConf(InternalParams.BAMValidationStringency, "SILENT")
		ss.sqlContext.setConf(InternalParams.UseIntelGKL, "true")
		ss.sqlContext.setConf(InternalParams.IOReadAlignmentMethod, "hadoopBAM")

		var query =
			s"""
				 |SELECT *
				 |FROM flagstat("$tableNameOrPath")
      """.stripMargin
		var tableNameBAM = "flagStatReads";
		if(sampleId == null) {
			ss.sql(s"""DROP  TABLE IF  EXISTS $tableNameBAM""")
			ss.sql(s"""
							|CREATE TABLE $tableNameBAM
							|USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
							|OPTIONS(path "$tableNameOrPath")
							|
     	""".stripMargin)
			s"""
				 |SELECT *
				 |FROM flagstat("$tableNameBAM", "$sampleId")
      """.stripMargin
		}

		ss.sql(query);
	}
}