package org.biodatageeks.sequila.datasources.BAM

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.seqdoop.hadoop_bam.{CRAMBDGInputFormat, CRAMInputFormat}

class CRAMDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "CRAM"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    new BDGAlignmentRelation[CRAMBDGInputFormat](parameters("path"),parameters.get("refPath"))(sqlContext)
  }
}
