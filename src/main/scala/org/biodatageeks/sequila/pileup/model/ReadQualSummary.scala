package org.biodatageeks.sequila.pileup.model

import org.apache.spark.internal.Logging


case class ReadQualSummary (start: Int, end: Int,
                            qualsArray: Array[Byte],
                            debugReadName: String,
                            cigarDerivedConf: CigarDerivedConf
                            ) extends Logging{

  @inline
  def getBaseQualityForPosition(position: Int): Byte = {
    try {
      qualsArray(relativePosition(position))

    }
    catch {
      case e: Exception => {
        log.error(s"ReadName: ${debugReadName} position: ${position}, start: ${start}, end: ${`end`} " +
          s"insertPos: ${cigarDerivedConf.indelPositions.insertPositions.mkString(",")}" +
          s"delPos: ${cigarDerivedConf.indelPositions.delPositions.mkString(",")}," +
          s"hasClip: ${cigarDerivedConf.hasClip}, hasIndel: ${cigarDerivedConf.hasIndel}" +
          s"hasDel: ${cigarDerivedConf.hasDel}, leftClipLength:${cigarDerivedConf.leftClipLength}")
        'E'.toByte
      }
    }
    finally {

    }
  }

  @inline
  def overlapsPosition(pos: Int): Boolean = !hasDeletionOnPosition(pos) && start <= pos && end >= pos

  @inline
  def relativePosition(absPosition: Int): Int = {
    if(!cigarDerivedConf.hasClip)
      absPosition - start + inDelEventsOffset(absPosition)
    else
      absPosition - start + inDelEventsOffset(absPosition) + cigarDerivedConf.leftClipLength
  }

  @inline
  private def inDelEventsOffset(pos: Int): Int = {
    if (!cigarDerivedConf.hasIndel)
      return 0
    cigarDerivedConf
      .indelPositions
      .insertPositions
      .count(_ <= pos + cigarDerivedConf.leftClipLength) -
      cigarDerivedConf
      .indelPositions
      .delPositions
      .count(_ <= pos + cigarDerivedConf.leftClipLength)

  }

  @inline
  def hasDeletionOnPosition(pos: Int): Boolean = {
    if (!cigarDerivedConf.hasDel)
      false
    else {
      cigarDerivedConf
        .indelPositions
        .delPositions.contains(pos)
    }
  }
}

