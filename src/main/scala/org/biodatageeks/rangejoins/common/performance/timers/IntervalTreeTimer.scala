package org.biodatageeks.rangejoins.common.performance.timers

import org.bdgenomics.utils.instrumentation.Metrics

/**
  * Created by marek on 01/02/2018.
  */
object IntervalTreeTimer extends Metrics {
  val IntervalTreeBuild = timer("Driver - IntervalTree Build")
  val IntervalTreeLookup = timer("Worker -  IntervalTreeLookup")


  val IntervalTreeBreifyBuild = timer("Driver - IntervalTreeBreify Build")
  val IntervalTreeBreifyLookup = timer("Worker -  IntervalTreeBreifyLookup")

  val IntervalTreeHTSBuild = timer("Driver - IntervalTreeHTS Build")
  val IntervalTreeHTSLookup = timer("Worker -  IntervalTreeHTSLookup")


}
