package org.biodatageeks.sequila.rangejoins.NCList

case class Backpack[T](intervalArray: Array[(Interval[Int],T)], processedInterval: Interval[Int]) {

}
