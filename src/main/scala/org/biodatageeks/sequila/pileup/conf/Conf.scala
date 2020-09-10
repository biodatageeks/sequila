package org.biodatageeks.sequila.pileup.conf

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

object Conf {
  var includeBaseQualities = false
  var filterFlag = "1796"
  var isBinningEnabled = false
  var binSize = QualityConstants.DEFAULT_BIN_SIZE
  var qualityArrayLength = 0
  var maxQuality = QualityConstants.DEFAULT_MAX_QUAL
  var maxQualityIndex = QualityConstants.DEFAULT_MAX_QUAL + 1

  override def toString: String = {
    ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE)
  }
}
