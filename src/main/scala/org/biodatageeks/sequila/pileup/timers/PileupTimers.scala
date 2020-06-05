package org.biodatageeks.sequila.pileup.timers

import org.bdgenomics.utils.instrumentation.Metrics

object PileupTimers extends Metrics {

  val MapPartitionTimer = timer("MapPartitionTimer from PileupMethods")

  val BAMReadTimer = timer("BAMReadTimer from PileupMethods")
  val InitContigLengthsTimer = timer("InitContigLengthsTimer from PileupMethods")
  val ContigAggrTimer = timer("ContigAggrTimer from PileupMethods")

  val DQTimerTimer = timer("DQTimerTimer from PileupMethods")
  val HandleFirstContingTimer = timer("HandleFirstContingTimer from PileupMethods")
  val AggMapLookupTimer = timer("AggMapLookupTimer from PileupMethods")
  val PrepareOutupTimer = timer("PrepareOutupTimer from PileupMethods")


  val AccumulatorTimer = timer("AccumulatorTimer from PileupMethods")
  val AccumulatorAllocTimer = timer("AccumulatorAllocTimer from PileupMethods")
  val AccumulatorRegisterTimer = timer("AccumulatorRegisterTimer from PileupMethods")


  val AccumulatorNestedTimer = timer("AccumulatorNestedTimer from PileupMethods")
  val BroadcastTimer = timer("BroadcastTimer from PileupMethods")
  val AdjustedEventsTimer = timer("AdjustedEventsTimer from PileupMethods")

  //events
  val EventsToPileupTimer = timer("EventsToPileupTimer from PileupMethods")
  val EventsGetBasesTimer = timer("EventsGetBasesTimer from PileupMethods")
  val CreateBlockPileupRowTimer = timer("CreateBlockPileupRowTimer from PileupMethods")


  //tails
  val TailCovTimer = timer("TailCovTimer from PileupMethods")
  val TailAltsTimer = timer("TailAltsTimer from PileupMethods")
  val TailEdgeTimer = timer("TailEdgeTimer from PileupMethods")

  //calculate broadacast
  val CalculateEventsTimer = timer("CalculateEventsTimer from PileupMethods")
  val CalculateAltsTimer = timer("CalculateAltsTimer from PileupMethods")

  val ShrinkArrayTimer = timer("ShrinkArrayTimer from PileupMethods")
  val ShrinkAltsTimer = timer("ShrinkAltsTimer from PileupMethods")

  //analyze
  val AnalyzeReadsTimer = timer("AnalyzeReadsTimer from PileupMethods")
  val AnalyzeReadsCalculateEventsTimer = timer("AnalyzeReadsCalculateEventsTimer from PileupMethods")
  val AnalyzeReadsCalculateAltsTimer = timer("AnalyzeReadsCalculateAltsTimer from PileupMethods")
  val AnalyzeReadsUpdateMaxReadLenInContigTimer = timer("AnalyzeReadsUpdateMaxReadLenInContigTimer from PileupMethods")

  //


  val AnalyzeReadsCalculateAltsMDTagParserTimer = timer("AnalyzeReadsCalculateAltsMDTagParserTimer from PileupMethods")





}
