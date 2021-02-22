package org.biodatageeks.sequila.pileup.converters.common

import org.apache.spark.sql.DataFrame
import org.biodatageeks.sequila.utils.Columns

trait PileupConverter {
  /**
   * Transforms input DF to common format (per-base output and string values for alts and quals)
   * @param df
   * @param caseSensitive
   * @return
   */
  def transformToCommonFormat(df:DataFrame, caseSensitive:Boolean): DataFrame

  /**
   * Transforms map fields into properly formatted and ordered string fields using UDFs
   * @param df
   * @return
   */
  def mapColumnsAsStrings(df: DataFrame): DataFrame = {
    val indA = df.columns.indexOf({Columns.ALTS})
    val indQ = df.columns.indexOf({Columns.QUALS})

    val outputColumns =  df.columns
    outputColumns(indA) = s"altmap_to_str(alts_to_char(${Columns.ALTS})) as ${Columns.ALTS}"
    outputColumns(indQ) = s"qualsmap_to_str(quals_to_char(${Columns.QUALS})) as ${Columns.QUALS}"

    val convertedDf = df.selectExpr(outputColumns: _*)
    convertedDf
  }
}
