package org.biodatageeks.sequila.pileup.serializers

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable

class CustomKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[mutable.LongMap[mutable.HashMap[Byte,Short]]], new LongMapSerializer())
    kryo.register(classOf[mutable.IntMap[mutable.HashMap[Byte,Short]]], new IntMapSerializer())
    kryo.register(classOf[mutable.LinkedHashSet[Int]])
  }
}
