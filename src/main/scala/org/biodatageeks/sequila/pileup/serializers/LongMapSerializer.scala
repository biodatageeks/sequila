package org.biodatageeks.sequila.pileup.serializers

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import scala.collection.mutable


class LongMapSerializer extends Serializer [mutable.LongMap[mutable.HashMap[Byte,Short]]]{


  override def write(kryo: Kryo, output: Output, map: mutable.LongMap[mutable.HashMap[Byte,Short]]): Unit = {
    output.writeInt(map.size, true)
    map.foreach { t =>
      kryo.writeClassAndObject(output, t)
    }
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[mutable.LongMap[mutable.HashMap[Byte,Short]]]): mutable.LongMap[mutable.HashMap[Byte,Short]] = {
    val size = input.readInt(true)

    var idx = 0
    val map = mutable.LongMap.empty[mutable.HashMap[Byte,Short]]

    while (idx < size) {
      val item = kryo.readClassAndObject(input).asInstanceOf[(Long,mutable.HashMap[Byte,Short] )]
      map.update(item._1, item._2)
      idx += 1
    }
    map
  }

}
