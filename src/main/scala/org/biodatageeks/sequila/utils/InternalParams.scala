package org.biodatageeks.sequila.utils

object InternalParams {

  /*A source directory of a table involded in BAM CTAS operation*/
  final val BAMCTASDir = "spark.biodatageeks.bam.bam_ctas_dir"
  final val BAMCTASFilter = "spark.biodatageeks.bam.bam_ctas_filter"
  final val BAMCTASLimit = "spark.biodatageeks.bam.bam_ctas_limit"
  final val BAMCTASHeaderPath =  "spark.biodatageeks.bam.bam_header_path"
  final val BAMCTASOutputPath = "spark.biodatageeks.bam.output_path"
  final val BAMCTASCmd = "spark.biodatageeks.bam.bam_ctas_cmd"


  /* parameter determining whether all positions in contig are included in output */
  final val ShowAllPositions = "spark.biodatageeks.coverage.allPositions"

  final val filterReadsByFlag = "spark.biodatageeks.coverage.filterFlag"

  final val RDDEventsName = "spark.biodatageeks.events"
  final val RDDPileupEventsName = "spark.biodatageeks.pileup.events"

  final val InputSplitSize = "spark.biodatageeks.bam.splitSize"

  final val useVectorizedOrcWriter = "spark.biodatageeks.pileup.useVectorizedOrcWriter"


  /*disq support*/
  final val IOReadAlignmentMethod = "spark.biodatageeks.readAligment.method"

  /*Intel GKL support */
  final val UseIntelGKL = "spark.biodatageeks.bam.useGKLInflate"

  /*interval queries*/
  final val AlignmentIntervals = "spark.biodatageeks.bam.intervals"

  final val BAMValidationStringency = "spark.biodatageeks.bam.validation"

  /*instrumentation*/

  final val EnableInstrumentation = "spark.biodatageeks.instrumentation"

  /* serialization mode */

  final val SerializationMode = "spark.biodatageeks.serialization"

  /* max base quality value, depends on sequencing technology. Default = 40 (Illumina 1.5) */
  // TODO add to documentation
  final val maxBaseQualityValue = "spark.biodatageeks.pileup.maxBaseQuality"

  /*interval joins*/
  final val useJoinOrder = "spark.biodatageeks.rangejoin.useJoinOrder"
  final val maxBroadCastSize = "spark.biodatageeks.rangejoin.maxBroadcastSize"
  final val maxGap = "spark.biodatageeks.rangejoin.maxGap"
  final val minOverlap = "spark.biodatageeks.rangejoin.minOverlap"
  final val intervalHolderClass = "spark.biodatageeks.rangejoin.intervalHolderClass"



}
