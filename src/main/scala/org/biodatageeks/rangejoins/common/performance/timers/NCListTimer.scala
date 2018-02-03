package org.biodatageeks.rangejoins.common.performance.timers

import org.bdgenomics.utils.instrumentation.Metrics

/**
  * Created by marek on 01/02/2018.
  */
object NCListTimer extends  Metrics{

  val NCListBuild = timer("Driver - org.biodatageeks.rangejoins.methods.NCList Build")
  val NCListLookup = timer("Worker -  NCListLookup")


}
