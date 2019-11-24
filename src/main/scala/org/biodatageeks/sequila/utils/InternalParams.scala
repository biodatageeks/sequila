package org.biodatageeks.sequila.utils

object InternalParams {

  /*A source directory of a table involded in BAM CTAS operation*/
  final val BAMCTASDir = "spark.biodatageeks.bam.bam_ctas_dir"
  final val BAMCTASFilter = "spark.biodatageeks.bam.bam_ctas_filter"
  final val BAMCTASLimit = "spark.biodatageeks.bam.bam_ctas_limit"
  final val BAMCTASHeaderPath =  "spark.biodatageeks.bam.bam_header_path"
  final val BAMCTASOutputPath = "spark.biodatageeks.bam.output_path"
  final val BAMCTASCmd = "spark.biodatageeks.bam.bam_ctas_cmd"

  /*refenenced column*/
  final val SAMPLE_COLUMN_NAME = "sampleId"

  /* parameter determining whether all positions in contig are included in output */
  final val ShowAllPositions = "spark.biodatageeks.coverage.allPositions"

  final val filterReadsByFlag = "spark.biodatageeks.coverage.filterFlag"

  final val RDDEventsName = "spark.biodatageeks.events"

  final val InputSplitSize = "spark.biodatageeks.bam.splitSize"


  /*disq support*/
  final val IOReadAlignmentMethod = "spark.biodatageeks.readAligment.method"

  /*Intel GKL support */
  final val UseIntelGKL = "spark.biodatageeks.bam.useGKLInflate"

}
