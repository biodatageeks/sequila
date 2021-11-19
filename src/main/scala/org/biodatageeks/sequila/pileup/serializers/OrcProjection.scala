package org.biodatageeks.sequila.pileup.serializers

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.biodatageeks.sequila.utils.Columns
import org.apache.orc.TypeDescription
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, IntegerType, MapType, ShortType, StringType}

object OrcProjection {

  def catalystToOrcSchema(attrs: Seq[Attribute]) = {
    val schema = TypeDescription.createStruct
    for (a <- attrs) {
      a.dataType match {
        case MapType(k, v, valueContainsNull) => {
          v match {
            case ArrayType(elementType, containsNull) =>{
              schema.addField(a.name, TypeDescription.createMap(convertBasicType(k), TypeDescription.createBinary()) )
            }
            case _ => schema.addField(a.name, TypeDescription.createMap(convertBasicType(k), convertBasicType(v)))
          }
        }
        case _ => addField(schema, a )
      }
    }
    schema
  }

  private def addField(schema: TypeDescription, attr: Attribute ) = {
    schema.addField(attr.name, convertBasicType(attr.dataType))
  }

  private def convertBasicType(dataType: DataType) = {
    dataType match {
      case StringType => TypeDescription.createString()
      case IntegerType => TypeDescription.createInt()
      case ShortType => TypeDescription.createShort()
      case ByteType => TypeDescription.createByte()
      case _ => throw new Exception("Unsupported CatalystType")
    }
  }

}
