package org.biodatageeks.sequila.pileup.conf

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

 class Conf extends Serializable {
  var includeBaseQualities: Boolean = false
  var filterFlag: String = "1796"
  var isBinningEnabled: Boolean = false
  var binSize: Int = QualityConstants.DEFAULT_BIN_SIZE
  var qualityArrayLength: Int = 0
  var maxQuality:Int = QualityConstants.DEFAULT_MAX_QUAL
  var maxQualityIndex:Int =  QualityConstants.DEFAULT_MAX_QUAL + 1

  var unknownContigName: String = "UCN"
  var coverageOnly: Boolean = false
  var outputFieldsNum: Int = 9
  var useVectorizedOrcWriter: Boolean = false
  var vectorizedOrcWriterPath: String = _
  var orcCompressCodec:String = _

  override def toString: String = {
    ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE)
  }
}
