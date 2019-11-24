package org.biodatageeks.sequila.datasources.ADAM

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class ADAMDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "BAM"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    new ADAMRelation(parameters("path"))(sqlContext)

}
