package org.biodatageeks.sequila.flagStat

import org.apache.spark.sql.{DataFrame, SequilaSession, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.sequila.utils.InternalParams
import org.slf4j.LoggerFactory

object FlagStatDebuggerEntryPoint {
	val sampleId = "NA12878.proper.wes.md"
	val bamFilePath: String = s"/Users/mwiewior/research/data/WES/${sampleId}.bam";



	def main(args: Array[String]): Unit = {
		performance(bamFilePath, null);
	}

	def performance(tableNameOrPath: String, sampleId: String): Unit = {
		System.setSecurityManager(null);
		val spark = SparkSession
			.builder()
			.master("local[4]")
			.config("spark.driver.memory","8g")
			.config("spark.biodatageeks.bam.validation", "SILENT")
			.config("spark.biodatageeks.readAligment.method", "hadoopBAM")
			.config("spark.biodatageeks.bam.useGKLInflate", "true")
			.getOrCreate();
		val ss = SequilaSession(spark);
		ss.time {
			ss.flagStat(tableNameOrPath, sampleId).show();
		}
		ss.stop()
	}
}