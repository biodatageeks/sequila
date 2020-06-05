package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SequilaSession
import org.biodatageeks.sequila.utils.SequilaRegister
import org.scalatest.{BeforeAndAfter, FunSuite}

class PileupTestSuite  extends FunSuite
  with DataFrameSuiteBase
  with BeforeAndAfter
  with SharedSparkContext {

  val bamMultiPath: String =
    getClass.getResource("/multichrom/mdbam/NA12878.multichrom.md.bam").getPath

  val referencePath = getClass.getResource("/reference/Homo_sapiens_assembly18_chr1_chrM.small.fasta").getPath

  val tableNameMultiBAM = "readsMulti"
  val splitSize = "1000000"



  before {
    spark.sparkContext.setLogLevel("ERROR")


    spark.sql(s"DROP TABLE IF EXISTS $tableNameMultiBAM")
    spark.sql(s"""
                 |CREATE TABLE $tableNameMultiBAM
                 |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
                 |OPTIONS(path "$bamMultiPath")
                 |
      """.stripMargin)

    val mapToString = (map: Map[Byte, Short]) => {
      if (map == null)
        "null"
      else
        map.map({
          case (k, v) => k.toChar -> v}).mkString.replace(" -> ", ":")
    }

    val byteToString = ((byte: Byte) => byte.toString)

    spark.udf.register("mapToString", mapToString)
    spark.udf.register("byteToString", byteToString)
  }


//
  test("Normal split") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")
    val query =
      s"""
         |SELECT contig, pos_start, pos_end, ref, coverage, mapToString(alts)
         |FROM  pileup('$tableNameMultiBAM', '$referencePath')
                 """.stripMargin
    val result = ss.sql(query)
    result.show(100, truncate = false)
  }
//
////    result.write.format("csv").save("normal_split")
//
//      assert(result.first().get(1) != 1) // first position check (should not start from 1 with ShowAllPositions = false)
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
//          .first()
//          .getShort(3) == 2) // value check
//      assert(
//        result.where(s"${Columns.CONTIG}='1' and ${Columns.START} == 88").first().getShort(3) == 7)
//
//      result.where(s"${Columns.CONTIG}='MT' and ${Columns.START} < 15").show()
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7")
//          .first()
//          .getShort(3) == 1) // value check [partition boundary]
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7881")
//          .first()
//          .getShort(3) == 248) // value check [partition boundary]
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7882")
//          .first()
//          .getShort(3) == 247) // value check [partition boundary]
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7883")
//          .first()
//          .getShort(3) == 246) // value check [partition boundary]
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 14402")
//          .first()
//          .getShort(3) == 182) // value check [partition boundary]
//      assert(
//        result
//          .groupBy(s"${Columns.CONTIG}")
//          .max(s"${Columns.START}")
//          .where(s"${Columns.CONTIG} == '1'")
//          .first()
//          .get(1) == 10066) // max value check
//      assert(
//        result
//          .groupBy(s"${Columns.CONTIG}")
//          .max(s"${Columns.START}")
//          .where(s"${Columns.CONTIG} == 'MT'")
//          .first()
//          .get(1) == 16571) // max value check
//      assert(
//        result
//          .groupBy(s"${Columns.CONTIG}", s"${Columns.START}")
//          .count()
//          .where("count != 1")
//          .count == 0) // no duplicates check
//    assert(
//      result
//        .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 34")
//        .first()
//        .getString(2) == "C") // Ref check
//    assert(
//      result
//        .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
//        .first()
//        .getString(2) == "C") // Ref check
//    assert(
//      result
//        .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 36")
//        .first()
//        .getString(2) == "C") // Ref check
//    assert(
//      result
//        .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 37")
//        .first()
//        .getString(2) == "T") // Ref check
//
//    }


//    test("Pileup  changed split size") {
//      spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
//      val ss = SequilaSession(spark)
//      SequilaRegister.register(ss)
//      ss.sparkContext.setLogLevel("ERROR")
//      val query =
//        s"""
//           | SELECT contig, pos_start, upper(ref), coverage, mapToString(alts)
//           |FROM  pileup('$tableNameMultiBAM', '${referencePath}')
//         """.stripMargin
//      val result = ss.sql(query)
//      result.show()
//  //    result.write.format("csv").save("changed_split")
//
////
//      assert(result.first().get(1) != 1) // first position check (should not start from 1 with ShowAllPositions = false)
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
//          .first()
//          .getShort(3) == 2) // value check
////      assert(
////        result.where(s"${Columns.CONTIG}='1' and ${Columns.START} == 88").first().getShort(3) == 7)
////
////      assert(
////        result
////          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7")
////          .first()
////          .getShort(3) == 1) // value check [partition boundary]
////      assert(
////        result
////          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7881")
////          .first()
////          .getShort(3) == 248) // value check [partition boundary]
////      assert(
////        result
////          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7882")
////          .first()
////          .getShort(3) == 247) // value check [partition boundary]
////      assert(
////        result
////          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 7883")
////          .first()
////          .getShort(3) == 246) // value check [partition boundary]
////      assert(
////        result
////          .where(s"${Columns.CONTIG}='MT' and ${Columns.START} == 14402")
////          .first()
////          .getShort(3) == 182) // value check [partition boundary]
////      assert(
////        result
////          .groupBy(s"${Columns.CONTIG}")
////          .max(s"${Columns.START}")
////          .where(s"${Columns.CONTIG} == '1'")
////          .first()
////          .get(1) == 10066) // max value check
////      assert(
////        result
////          .groupBy(s"${Columns.CONTIG}")
////          .max(s"${Columns.START}")
////          .where(s"${Columns.CONTIG} == 'MT'")
////          .first()
////          .get(1) == 16571) // max value check
////      assert(
////        result
////          .groupBy(s"${Columns.CONTIG}", s"${Columns.START}")
////          .count()
////          .where("count != 1")
////          .count == 0) // no duplicates check
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 34")
//          .first()
//          .getString(2) == "C") // Ref check
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 35")
//          .first()
//          .getString(2) == "C") // Ref check
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 36")
//          .first()
//          .getString(2) == "C") // Ref check
//      assert(
//        result
//          .where(s"${Columns.CONTIG}='1' and ${Columns.START} == 37")
//          .first()
//          .getString(2) == "T") // Ref check
//
//    }


//    test("Pileup multi chrom check"){
//      val  ss = SequilaSession(spark)
//      SequilaRegister.register(ss)
//      ss.sparkContext.setLogLevel("ERROR")
//      val query =
//        s"""
//           |SELECT distinct contig
//           |FROM  pileup('$tableNameMultiBAM', '${referencePath}')
//         """.stripMargin
//      val result = ss.sql(query)
//      result.show()
//      assert(result.count() == 2)
//    }
}
