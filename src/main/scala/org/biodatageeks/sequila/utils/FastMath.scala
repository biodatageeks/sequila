package org.biodatageeks.sequila.utils

object FastMath {


  def sumShort(a: Array[Short]) = {
    var i = 0
    var cumSum = 0
    while(i < a.length){
      cumSum += a(i)
      i+=1
    }
    cumSum.toShort
  }

}
