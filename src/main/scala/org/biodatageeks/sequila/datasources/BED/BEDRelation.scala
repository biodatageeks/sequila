package org.biodatageeks.sequila.datasources.BED

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.biodatageeks.sequila.utils.{Columns, DataQualityFuncs}


class BEDRelation(path: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with Serializable {

  @transient val logger = Logger.getLogger(this.getClass.getCanonicalName)
  override def schema: org.apache.spark.sql.types.StructType = Encoders.product[org.biodatageeks.formats.BrowserExtensibleData].schema

  private def getValueFromColumn(colName:String, r:Array[String]): Any = {
    colName match {
      case Columns.CONTIG       =>  DataQualityFuncs.cleanContig(r(0) )
      case Columns.START        =>  r(1).toInt + 1 //Convert interval to 1-based
      case Columns.END          =>  r(2).toInt
      case Columns.NAME         =>  if (r.length > 3) r(3) else null
      case Columns.SCORE        =>  if (r.length > 4) r(4).toInt else null
      case Columns.STRAND       =>  if (r.length > 5) r(5) else null
      case Columns.THICK_START  =>  if (r.length > 6) r(6).toInt else null
      case Columns.THICK_END    =>  if (r.length > 7) r(7).toInt else null
      case Columns.ITEM_RGB     =>  if (r.length > 8) r(8).split(",").map(_.toInt) else null
      case Columns.BLOCK_COUNT  =>  if (r.length > 9) r(9).toInt else null
      case Columns.BLOCK_SIZES  =>  if (r.length > 10) r(10).split(",").map(_.toInt) else null
      case Columns.BLOCK_STARTS =>  if (r.length > 11) r(11).split(",").map(_.toInt) else null
      case _                    =>  throw new Exception(s"Unknown column found: ${colName}")
    }
  }
  override def buildScan(requiredColumns:Array[String], filters:Array[Filter]): RDD[Row] = {
    sqlContext
      .sparkContext
      .textFile(path)
      .filter(!_.toLowerCase.startsWith("track"))
      .filter(!_.toLowerCase.startsWith("browser"))
      .map(_.split("\t"))
      .map(r=>
            {
              val record = new Array[Any](requiredColumns.length)
              //requiredColumns.
              for (i <- 0 to requiredColumns.length - 1) {
                record(i) = getValueFromColumn(requiredColumns(i), r)
              }
              Row.fromSeq(record)
            }

    )




  }

}
