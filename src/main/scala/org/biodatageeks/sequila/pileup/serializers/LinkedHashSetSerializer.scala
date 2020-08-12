package org.biodatageeks.sequila.pileup.serializers

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

import scala.collection.mutable

class LinkedHashSetSerializer extends Serializer [mutable.LinkedHashSet[Int]]{
  override def write(kryo: Kryo, output: Output, set: mutable.LinkedHashSet[Int]): Unit = {
    output.writeInt(set.size, true)
    set.foreach { t =>
      kryo.writeClassAndObject(output, t)
    }
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[mutable.LinkedHashSet[Int]]): mutable.LinkedHashSet[Int] = {
    val size = input.readInt(true)

    var idx = 0
    val set = mutable.LinkedHashSet[Int]()

    while (idx < size) {
      val item = kryo.readClassAndObject(input).asInstanceOf[Int]
      set.add(item)
      idx += 1
    }
    set
  }
}
