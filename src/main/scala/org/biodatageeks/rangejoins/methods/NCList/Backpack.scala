package org.biodatageeks.rangejoins.NCList

case class Backpack[T](intervalList: List[(Interval[Int],T)], processedInterval: Interval[Int]) {

}
