package org.biodatageeks.sequila.pileup.serializers

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import scala.collection.mutable

class IntMapSerializer extends Serializer [mutable.IntMap[mutable.HashMap[Byte,Short]]] {
  override def write(kryo: Kryo, output: Output, map: mutable.IntMap[mutable.HashMap[Byte,Short]]): Unit = {
    output.writeInt(map.size, true)
    map.foreach { t =>
      kryo.writeClassAndObject(output, t)
    }
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[mutable.IntMap[mutable.HashMap[Byte,Short]]]): mutable.IntMap[mutable.HashMap[Byte,Short]] = {
    val size = input.readInt(true)

    var idx = 0
    val map = mutable.IntMap.empty[mutable.HashMap[Byte,Short]]

    while (idx < size) {
      val item = kryo.readClassAndObject(input).asInstanceOf[(Int,mutable.HashMap[Byte,Short] )]
      map.update(item._1, item._2)
      idx += 1
    }
    map
  }

}
