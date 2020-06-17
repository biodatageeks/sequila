package org.biodatageeks.sequila.pileup

import scala.collection.mutable

package object model {
  type SingleLocusAlts = mutable.HashMap[Byte,Short]
  val SingleLocusAlts = mutable.HashMap[Byte,Short] _

  type MultiLociAlts= mutable.LongMap[SingleLocusAlts]
  val MultiLociAlts = mutable.LongMap[SingleLocusAlts] _
}
