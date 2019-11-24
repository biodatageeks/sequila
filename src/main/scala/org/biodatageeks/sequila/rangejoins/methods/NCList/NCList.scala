package org.biodatageeks.sequila.rangejoins.NCList

import scala.collection.mutable.ArrayBuffer


case class NCList(var childrenBuf: ArrayBuffer[NCList], var nChildren: Int, var rgidBuf: ArrayBuffer[Int]) extends Serializable {

}
