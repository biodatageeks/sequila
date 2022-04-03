package org.biodatageeks.sequila.flagStat

import htsjdk.samtools.SAMRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.biodatageeks.sequila.datasources.BAM.{BAMFileReader, BAMTableReader}
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, CRAMBDGInputFormat}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.pileup.PileupMethods
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.utils.{FileFuncs, TableFuncs}

import scala.collection.mutable.ListBuffer

case class FlagStatRow(
	RCount: Long,
	QCFail: Long,
	DUPES: Long,
	MAPPED: Long,
	UNMAPPED: Long,
	PiSEQ: Long,
	Read1: Long,
	Read2: Long,
	PPaired: Long,
	WIaMM: Long,
	Singletons: Long
);

case class FlagStat(spark:SparkSession) {
	val Logger = LoggerFactory.getLogger(this.getClass.getCanonicalName);

	def processFile(bamFilePath: String) : DataFrame = {
		val tableReader = new BAMFileReader[BAMBDGInputFormat](spark, bamFilePath, null);
		val records = tableReader.readFile
		processDF(processRows(records))
	}

	def processRows(records: RDD[SAMRecord]) : RDD[(String, Long)] = {
		records.mapPartitions((m) => {
			var RCount = 0L;
			var QCFail = 0L;
			var DUPES = 0L;
			var UNMAPPED = 0L;
			var MAPPED = 0L;
			var PiSEQ = 0L;
			var Read1 = 0L;
			var Read2 = 0L;
			var PPaired = 0L;
			var WIaMM = 0L;
			var Singletons = 0L;

			while (m.hasNext) {
				val iter = m.next;

				if (iter.getReadFailsVendorQualityCheckFlag()) {
					QCFail += 1;
				}
				if (iter.getDuplicateReadFlag()) {
					DUPES += 1;
				}
				if (iter.getReadUnmappedFlag()) {
					UNMAPPED += 1;
				} else {
					MAPPED += 1;
				}
				if (iter.getReadPairedFlag()) {
					PiSEQ += 1;
					if (iter.getSecondOfPairFlag()) {
						Read2 += 1;
					} else if(iter.getFirstOfPairFlag()) {
						Read1 += 1;
					}
					if (iter.getProperPairFlag()) {
						PPaired += 1;
					}
					if (!iter.getReadUnmappedFlag() && !iter.getMateUnmappedFlag()) {
						WIaMM += 1;
					}
					if (!iter.getReadUnmappedFlag() && iter.getMateUnmappedFlag()) {
						Singletons += 1;
					}
				}
				RCount += 1;
			}

			//Iterator(Row(RCount, QCFail, DUPES, MAPPED, UNMAPPED, PiSEQ, Read1, Read2, PPaired, WIaMM, Singletons))
			Iterator(
				("RCount", RCount),
				("QCFail", QCFail),
				("DUPES", DUPES),
				("MAPPED", MAPPED),
				("UNMAPPED", UNMAPPED),
				("PiSEQ", PiSEQ),
				("Read1", Read1),
				("Read2", Read2),
				("PPaired", PPaired),
				("WIaMM", WIaMM),
				("Singletons", Singletons)
			)
		}).reduceByKey((v1, v2) => v1 + v2)
	}
	def processDF(rows: RDD[(String, Long)]): DataFrame = {
		var mapping = rows.collectAsMap();
		var sequence = new ListBuffer[Long];
		FlagStat.Schema.fieldNames.foreach(x => {
			sequence += mapping.get(x).get;
		})
		val result = Row.fromSeq(sequence);
		val rdd = spark.sparkContext.parallelize(Seq(result));
		spark.createDataFrame(rdd, FlagStat.Schema);
	}


	def handleFlagStat(tableNameOrPath: String, sampleId: String): RDD[(String, Long)] = {
		if(sampleId != null)
			Logger.info(s"Calculating flagStat on table: $tableNameOrPath")
		else
			Logger.info(s"Calculating flagStat using file: $tableNameOrPath")

		val (records) = {
			if (sampleId != null) {
				val metadata = TableFuncs.getTableMetadata(spark, tableNameOrPath)
				val tableReader = metadata.provider match {
					case Some(f) if sampleId != null =>
						if (f == InputDataType.BAMInputDataType)
							new BAMTableReader[BAMBDGInputFormat](spark, tableNameOrPath, sampleId, "bam", None)
						else throw new Exception("Only BAM file format is supported.")
					case None => throw new Exception("Empty file extension - BAM file format is supported..")
				}
				tableReader
					.readFile
			}
			else {
				val fileReader = FileFuncs.getFileExtension(tableNameOrPath) match {
					case "bam" => new BAMFileReader[BAMBDGInputFormat](spark, tableNameOrPath, None)
				}
				fileReader
					.readFile
			}
		}

		processRows(records);
	}
}

object FlagStat {
	val Schema = StructType(Array(
		StructField("RCount", LongType, false),
		StructField("QCFail", LongType, false),
		StructField("DUPES", LongType, false),
		StructField("MAPPED", LongType, false),
		StructField("UNMAPPED", LongType, false),
		StructField("PiSEQ", LongType, false),
		StructField("Read1", LongType, false),
		StructField("Read2", LongType, false),
		StructField("PPaired", LongType, false),
		StructField("WIaMM", LongType, false),
		StructField("Singletons", LongType, false)
	));
}