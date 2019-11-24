package org.biodatageeks.sequila.outputformats

import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.seqdoop.hadoop_bam.{KeyIgnoringBAMOutputFormat, KeyIgnoringBAMRecordWriter, SAMRecordWritable}
import org.biodatageeks.sequila.utils.InternalParams
import org.apache.hadoop.fs.Path


class BAMOutputFormat[K] extends KeyIgnoringBAMOutputFormat[K] with Serializable {
  setWriteHeader(true)

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, SAMRecordWritable] = {
    val conf = context.getConfiguration

    // source BAM file to get the header from and the output BAM for writing
    val inPath = new Path(conf.get(InternalParams.BAMCTASHeaderPath))
    val outPath = new Path(conf.get(InternalParams.BAMCTASOutputPath))

    readSAMHeaderFrom(inPath, conf)

    // now that we have the header set, we need to make a record reader
   new KeyIgnoringBAMRecordWriter[K](outPath,header, true, context)

  }
}