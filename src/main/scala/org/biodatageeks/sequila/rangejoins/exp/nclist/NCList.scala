package org.biodatageeks.sequila.rangejoins.exp.nclist

import org.biodatageeks.sequila.rangejoins.methods.base.{BaseIntervalHolder, BaseNode}

import java.io._
import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class LListElem[V](var start: Int, var end: Int, var sublist: Int = -1, var h_idx: Int = -1) extends BaseNode[V] {
  var mValue: util.ArrayList[V] = new util.ArrayList[V]()

  def getStart: Int = {
    return start
  }

  def getEnd: Int = {
    return end
  }

  def setValue(v: V): Unit = {
    mValue.add(v)
  }

  def getValue: util.ArrayList[V] = {
    return mValue
  }

  def getValueRaw: V = {
    return mValue.get(0)
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[LListElem[V]]

  override def equals(that: Any): Boolean =
    that match {
      case that: LListElem[V] => {
        that.canEqual(this) && this.start == that.start && this.end == that.end
      }
      case _ => false
    }
}

class HListElem extends Serializable {var start: Int = 0; var length: Int = 0}


class NCList[V] extends BaseIntervalHolder[V] with Serializable {
  var buffer_l_list: mutable.HashMap[ (Int, Int), LListElem[V]] = mutable.HashMap[ (Int, Int), LListElem[V]]()
  var l_list: Array[LListElem[V]] = Array()
  var h_list: ArrayBuffer[HListElem] = ArrayBuffer()
  var built = false

  def iterator(): java.util.Iterator[BaseNode[V]] = ???

  override def put(start: Int, end: Int, value: V): V = {
    val ret: V = value


    val elem = new LListElem[V](start, end, -1)



    val i = buffer_l_list contains (start,end)
    if (i){
      buffer_l_list((start,end)).setValue(value)
    }
    else{
      elem.setValue(value)
      buffer_l_list((start,end)) = (elem)
    }
    ret
  }

  override def remove(start: Int, end: Int): V = ???

  override def find(start: Int, end: Int): BaseNode[V] = {
    val i = l_list.indexOf(new LListElem[V](start, end, -1))
    l_list(i)
  }

  override def overlappers(start: Int, end: Int): java.util.Iterator[BaseNode[V]] = {
    if (!built) {
      build_index()
    }
    val ret = ArrayBuffer[LListElem[V]]()
    overlaps(start, end, 0, ret)
    new Ite[V](ret.asInstanceOf[ArrayBuffer[BaseNode[V]]])
  }

  def find_overlap_start(start: Int, end: Int, sub_start: Int, sub_length: Int): Int = {
    var l = 0
    var mid = 0
    var r = sub_length - 1

    while (l < r) {
      mid = (l + r) / 2
      if (l_list(sub_start + mid).end < start) {
        l = mid + 1
      } else {
        r = mid
      }
    }

    if (l < sub_length && l_list(sub_start + l).start <= end && start <= l_list(sub_start + l).end) {
      sub_start + l
    } else {
      -1
    }
  }

  def overlaps(start: Int, end: Int, h_idx: Int, ret: ArrayBuffer[LListElem[V]]): Unit = {
    if (h_idx == -1) return
    val sublist_end = h_list(h_idx).start + h_list(h_idx).length

    var it = h_list(h_idx).start

    it = find_overlap_start(start, end, h_list(h_idx).start, h_list(h_idx).length)

    while (it >= 0 && it < sublist_end && l_list(it).start <= end && start <= l_list(it).end) {
      ret.append(l_list(it))
      overlaps(start, end, l_list(it).h_idx, ret)
      it = it + 1
    }
  }

  def contains(el1: LListElem[V], el2: LListElem[V]): Boolean = {
    el1.start <= el2.start && el2.end < el1.end
  }


  override def postConstruct(domains: Option[Int]):Unit = {
    build_index()
  }

  def build_index(): Unit = {
    l_list = buffer_l_list.toArray.map(v => v._2)
    buffer_l_list.clear()
    h_list.clear()
    l_list = l_list.sortWith((e1, e2) => e1.start < e2.start || contains(e1, e2))

    h_list += new HListElem()
    var parents: ArrayBuffer[LListElem[V]] = ArrayBuffer()
    var h_indices: ArrayBuffer[Int] = ArrayBuffer()

    h_indices += 0

    for (i <- l_list.indices) {
      while (parents.nonEmpty && !contains(parents.last, l_list(i))) {
        parents.remove(parents.length - 1)
        h_indices.remove(h_indices.length - 1)
      }
      l_list(i).sublist = h_indices.last
      if (i != l_list.length - 1 && contains(l_list(i), l_list(i + 1))) {
        parents += l_list(i)
        h_indices += h_list.length
        h_list += new HListElem()
        l_list(i).h_idx = h_indices.last
      }
    }
    l_list = l_list.sortWith((e1, e2) => e1.sublist < e2.sublist)

    var curr_idx = 0
    var l_start_idx = 0
    var num_intervals = 0

    for (i <- l_list.indices) {
      if (l_list(i).sublist == curr_idx) {
        num_intervals = num_intervals + 1
      } else {
        curr_idx = curr_idx + 1
        l_start_idx = i
        num_intervals = 1
      }
      h_list(curr_idx).start = l_start_idx
      h_list(curr_idx).length = num_intervals
    }
    built = true
  }

}

class Ite[V](array: ArrayBuffer[BaseNode[V]]) extends java.util.Iterator[org.biodatageeks.sequila.rangejoins.methods.base.BaseNode[V]] {
  var idx = 0

  override def hasNext: Boolean = {
    return idx <= (array.length - 1)
  }

  override def next(): BaseNode[V] = {
    val ret = array(idx)
    idx = idx + 1
    return ret
  }
}

