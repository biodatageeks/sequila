package org.biodatageeks.sequila.datasources.VCF

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class VCFDataSource extends DataSourceRegister
  with RelationProvider {

  override def shortName(): String = "VCF"
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    new VCFRelation(parameters("path"),parameters.get("normalization_mode"), parameters.get("ref_genome_path"))(sqlContext)
  }

}
