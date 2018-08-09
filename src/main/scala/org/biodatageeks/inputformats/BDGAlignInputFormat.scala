package org.biodatageeks.inputformats

import org.apache.hadoop.io.LongWritable

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.seqdoop.hadoop_bam.{BAMBDGRecordReader, SAMRecordWritable}

abstract class BDGAlignInputFormat extends FileInputFormat[LongWritable, SAMRecordWritable] {

}
