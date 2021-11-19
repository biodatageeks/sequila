package org.biodatageeks.sequila.pileup.serializers

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.sequila.utils.Columns
import org.apache.orc.TypeDescription
import org.apache.spark.sql.types.{IntegerType, ShortType, StringType}

object OrcProjection {

  def catalystToOrcSchema(attrs: Seq[Attribute]) ={
    val schema = TypeDescription.createStruct
    for(a <- attrs) {
      if(a.dataType == StringType)
        schema.addField(a.name, TypeDescription.createString())
      else if (a.dataType == IntegerType)
        schema.addField(a.name, TypeDescription.createInt())
      else if (a.dataType == ShortType)
        schema.addField(a.name, TypeDescription.createShort())
      else
        throw new Exception("Unsupported CatalystType")
    }
    schema
  }

}
