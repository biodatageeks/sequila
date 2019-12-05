package org.biodatageeks.sequila.datasources.BED

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class BEDDataSource  extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "bed"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    new BEDRelation(parameters("path"))(sqlContext)
  }
}
