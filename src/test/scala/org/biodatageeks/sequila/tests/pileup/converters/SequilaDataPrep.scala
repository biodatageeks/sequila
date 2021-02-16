package org.biodatageeks.sequila.tests.pileup.converters

import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.pileup.Writer
import org.biodatageeks.sequila.pileup.converters.SamtoolsConverter
import org.biodatageeks.sequila.tests.pileup.PileupTestBase
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}



class SequilaDataPrep extends PileupTestBase {

  val mapToString = (map: Map[Byte, Short]) => {
    if (map == null)
      "null"
    else
      map.map({
        case (k, v) => k.toChar -> v
      }).toSeq.sortBy(_._1).mkString.replace(" -> ", ":")
  }

  val nestedMapToString = (map: Map[String, Map[String, Short]]) => {
    if (map == null)
      "null"
    else
      map.map({
        case (k, v) => k -> v.toSeq.sortBy(_._1)
      }).toSeq.sortBy(_._1).mkString.replaceAll("ArrayBuffer", "")
  }

  val queryQual =
    s"""
       |SELECT contig, pos_start, pos_end, ref, coverage, mapToString(alts) as alts , nestedMapToString(to_charmap(${Columns.QUALS})) as ${Columns.QUALS}
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true)
                         """.stripMargin


  test("alts,quals: one partition") {

    spark.udf.register("nestedMapToString", nestedMapToString)
    spark.udf.register("mapToString", mapToString)

    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)

    val bdgRes = ss.sql(queryQual).orderBy("contig", "pos_start")

    Writer.saveToCsvFileWithQuals(ss, bdgRes, "sequilaQualsBlocks.csv")
  }

}
