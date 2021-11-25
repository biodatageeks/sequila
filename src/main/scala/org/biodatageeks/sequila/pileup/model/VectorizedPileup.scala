package org.biodatageeks.sequila.pileup.model

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ListColumnVector, LongColumnVector, MapColumnVector, VectorizedRowBatch}
import org.apache.orc.{OrcFile, Writer}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.serializers.OrcProjection


case class VectorizedPileup (
                              fullMode: Boolean,
                              writer: Writer,
                              batch: VectorizedRowBatch,
                              contigVector: BytesColumnVector,
                              postStartVector:LongColumnVector,
                              postEndVector: LongColumnVector,
                              refVector:BytesColumnVector,
                              covVector: LongColumnVector,
                              countRefVector:LongColumnVector,
                              countNonRefVector:LongColumnVector,
                              altsMapVector:MapColumnVector,
                              altsMapKey: LongColumnVector,
                              altsMapValue: LongColumnVector,
                              qualsMapVector: MapColumnVector,
                              qualsMapKey: LongColumnVector,
                              qualsMapValue: ListColumnVector,
                              altsMapSize: Int,
                              qualityArraySize: Int
                            ) {

  def QUALITY_ARRAY_SIZE: Int = qualityArraySize
  def ALTS_MAP_SIZE: Int = altsMapSize


}

object VectorizedPileup {

  def create(fullMode: Boolean, conf:Conf, output: Seq[Attribute], index:Int):VectorizedPileup = {

    val hadoopConf = new Configuration()
    if(conf.orcCompressCodec != null)
      hadoopConf.set("orc.compress", conf.orcCompressCodec)
    val path = conf.vectorizedOrcWriterPath
    val schema = OrcProjection.catalystToOrcSchema(output)
    val writer = OrcFile
      .createWriter(new Path(s"${path}/part-${index}-${System.currentTimeMillis()}.${conf.orcCompressCodec.toLowerCase}.orc"),
        OrcFile.writerOptions(hadoopConf).setSchema(schema))
    val batch = schema.createRowBatch
    val contigVector = batch.cols(0).asInstanceOf[BytesColumnVector]
    val postStartVector = batch.cols(1).asInstanceOf[LongColumnVector]
    val postEndVector = batch.cols(2).asInstanceOf[LongColumnVector]
    val refVector = batch.cols(3).asInstanceOf[BytesColumnVector]
    val covVector = batch.cols(4).asInstanceOf[LongColumnVector]

    val BATCH_SIZE = batch.getMaxSize
    val ALTS_MAP_SIZE = conf.altsMapSize
    val QUALITY_ARRAY_SIZE = conf.maxQualityIndex

    if (fullMode) {
      val countRefVector = batch.cols(5).asInstanceOf[LongColumnVector]
      val countNonRefVector = batch.cols(6).asInstanceOf[LongColumnVector]
      //alts
      val altsMapVector = batch.cols(7).asInstanceOf[MapColumnVector]
      val altsMapKey = altsMapVector.keys.asInstanceOf[LongColumnVector]
      val altsMapValue = altsMapVector.values.asInstanceOf[LongColumnVector]
      altsMapKey.ensureSize(BATCH_SIZE * ALTS_MAP_SIZE, false)
      altsMapValue.ensureSize(BATCH_SIZE * ALTS_MAP_SIZE, false)
      //quals
      val qualsMapVector = batch.cols(8).asInstanceOf[MapColumnVector]
      val qualsMapKey = qualsMapVector.keys.asInstanceOf[LongColumnVector]
      val qualsMapValue = qualsMapVector.values.asInstanceOf[ListColumnVector]
      qualsMapKey.ensureSize(BATCH_SIZE * ALTS_MAP_SIZE, false)
      qualsMapValue.ensureSize(BATCH_SIZE * ALTS_MAP_SIZE, false)
      qualsMapVector.values.asInstanceOf[ListColumnVector].child.ensureSize(BATCH_SIZE * ALTS_MAP_SIZE * QUALITY_ARRAY_SIZE, false)
      new VectorizedPileup(fullMode, writer, batch, contigVector, postStartVector, postEndVector, refVector, covVector, countRefVector, countNonRefVector,
        altsMapVector, altsMapKey, altsMapValue, qualsMapVector, qualsMapKey, qualsMapValue, ALTS_MAP_SIZE, QUALITY_ARRAY_SIZE)
    } else
      new VectorizedPileup(
        fullMode, writer, batch, contigVector, postStartVector,
        postEndVector, refVector, covVector, null, null,
        null, null, null,
        null, null, null, ALTS_MAP_SIZE, QUALITY_ARRAY_SIZE)
  }

}
