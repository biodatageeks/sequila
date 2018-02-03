package org.biodatageeks.rangejoins.NCList

case class NCList(var childrenBuf: Array[NCList],var nChildren: Int, var rgidBuf: Array[Int]) extends Serializable {

}
