package org.biodatageeks.sequila.flagStat

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.sequila.datasources.BAM.{BAMFileReader, BAMTableReader}
import org.seqdoop.hadoop_bam.BAMBDGInputFormat
import org.slf4j.LoggerFactory
import scala.collection.mutable.Map
class FlagStat(spark:SparkSession) {
	val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  	var readCount = accumulate("Read Count");
		var QC_failure = accumulate("Vendor Quality-Check Failures");
		var duplicates = accumulate("Duplicate Count");
		var mapped = accumulate("Mapped Entries");
		var paired_in_sequencing = accumulate("Paired in sequencing");
		var read1 = accumulate("Read #1");
		var read2 = accumulate("Read #2");
		var properly_paired = accumulate("Properly Pairsed");
		var with_itself_and_mate_mapped = accumulate("With itself and mate mapped");
		var singletons = accumulate("Singletons");
	// No acessors:
	//var with_mate_mapped_to_a_different_chr = accumulate("With mate mapped to a different chr");
	//var with_mate_mapped_to_a_different_chr_maq_greaterequal_than_5 = accumulate("With mate mapped to a different chr (mapQ>=5)");

	def getFlagStat(bamFilePath: String, sampleId: String) : Unit = {
		val tableReader = new BAMTableReader[BAMBDGInputFormat](spark, bamFilePath, sampleId, "bam", None)
		val records = tableReader.readFile

		records.foreach(
			(iter) => {
				readCount.add(1L);
				if (iter.getReadFailsVendorQualityCheckFlag()) {
					QC_failure.add(1L);
				}
				if (iter.getDuplicateReadFlag()) {
					duplicates.add(1L);
				}
				if (!iter.getReadUnmappedFlag()) {
					mapped.add(1L);
				}
				if (iter.getReadPairedFlag()) {
					paired_in_sequencing.add(1L);

					if (iter.getSecondOfPairFlag()) {
						read2.add(1L);
					} else if(iter.getFirstOfPairFlag()) {
						read1.add(1L);
					}

					if (iter.getProperPairFlag()) {
						properly_paired.add(1L);
					}
					if (!iter.getReadUnmappedFlag() && !iter.getMateUnmappedFlag()) {
						with_itself_and_mate_mapped.add(1L);
					}
					if (!iter.getReadUnmappedFlag() && iter.getMateUnmappedFlag()) {
						singletons.add(1L);
					}
				}
			}
		);

	}

	def accumulate(name : scala.Predef.String) : org.apache.spark.util.LongAccumulator = {
		new org.apache.spark.util.LongAccumulator()
	}

	def getFlagStatB(bamFilePath: String, sampleId: String) : Unit = {
		val tableReader = new BAMTableReader[BAMBDGInputFormat](spark, bamFilePath, sampleId, "bam", None)
		val records = tableReader.readFile;

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
			Iterator (Map[String,Long](
				"RCount" -> RCount,
				"QCFail" -> QCFail,
				"DUPES" -> DUPES,
				"MAPPED" -> MAPPED,
				"UNMAPPED" -> UNMAPPED,
				"PiSEQ" -> PiSEQ,
				"Read1" -> Read1,
				"Read2" -> Read2,
				"PPaired" -> PPaired,
				"WIaMM" -> WIaMM,
				"Singletons" -> Singletons
			)/*.toList/toSeq*/)
		}).reduceByKey();
	}


}
