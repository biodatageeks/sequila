package org.biodatageeks.sequila.pileup.broadcast

import org.biodatageeks.sequila.pileup.model.Alts._
import org.biodatageeks.sequila.pileup.model.QualityCache
import org.biodatageeks.sequila.pileup.model.Quals._


case class Tail(
                 contig: String,
                 minPos: Int,
                 startPoint: Int,
                 events: Array[Short],
                 alts: MultiLociAlts,
                 quals: MultiLociQuals,
                 cumSum: Short,
                 cache: QualityCache
               )