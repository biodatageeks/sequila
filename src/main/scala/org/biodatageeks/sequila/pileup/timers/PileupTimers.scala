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
  val PileupUpdateCreationTimer = timer("PileupUpdateCreationTimer from PileupMethods")
  val AccumulatorAddTimer = timer("AcumulatorAddTimer from PileupMethods")


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
  val CalculateEventsTimer = timer("CalculateEventsTimer from ContigAggregate")
  val CalculateAltsTimer = timer("CalculateAltsTimer from ContigAggregate")
  val CalculateQualsTimer = timer("CalculateQualsTimer from ContigAggregate")
  val FillQualityForHigherAltsTimer = timer("FillQualityForHigherAltsTimer from ContigAggregate")
  val FillQualityForLowerAltsTimer = timer("FillQualityForLowerAltsTimer from ContigAggregate")


  val ShrinkArrayTimer = timer("ShrinkArrayTimer from PileupMethods")
  val ShrinkAltsTimer = timer("ShrinkAltsTimer from PileupMethods")

  //analyze
  val AnalyzeReadsTimer = timer("AnalyzeReadsTimer from Read")
  val AnalyzeReadsCalculateEventsTimer = timer("AnalyzeReadsCalculateEventsTimer from Read")
  val AnalyzeReadsCalculateAltsTimer = timer("AnalyzeReadsCalculateAltsTimer from Read")
  val AnalyzeReadsCalculateQualsTimer = timer("AnalyzeReadsCalculateQualsTimer from Read")
  val AnalyzeReadsCalculateQualsFillQualsTimer = timer("AnalyzeReadsCalculateQualsFillQualsTimer from Read")
  val AnalyzeReadsCalculateQualsCheckRangeTimer = timer("AnalyzeReadsCalculateQualsCheckRangeTimer from Read")
  val AnalyzeReadsCalculateQualsAddToCacheTimer = timer("AnalyzeReadsCalculateQualsAddToCacheTimer from Read")
  val AnalyzeReadsCalculateQualsUpdateAltsTimer = timer("AnalyzeReadsCalculateQualsUpdateAltsTimer from Read")

  val FillPastQualitiesFromCacheTimer = timer ("FillPastQualitiesFromCacheTimer from Read")
  val AnalyzeReadsCalculateAltsParseMDTimer = timer("AnalyzeReadsCalculateAltsParseMDTimer from Read")
  val AnalyzeReadsUpdateMaxReadLenInContigTimer = timer("AnalyzeReadsUpdateMaxReadLenInContigTimer from PileupMethods")

  //


  val AnalyzeReadsCalculateAltsMDTagParserTimer = timer("AnalyzeReadsCalculateAltsMDTagParserTimer from PileupMethods")





}
