package org.biodatageeks.sequila.rangejoins.exp.iit

import java.util
import org.biodatageeks.sequila.rangejoins.methods.base.{BaseIntervalHolder, BaseNode}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class IITree[V] extends BaseIntervalHolder[V] with Serializable {
  private var nodes = new ArrayBuffer[Node[V]]()
  private var maxLevel: Int = -1
  private val map = mutable.HashMap[(Int, Int), Node[V]]()
  private var rootStack: Array[Int] = _
  private var root: Int = _
  private var stack: Array[Array[Int]] = _


  override def postConstruct(domains: Option[Int]): Unit = {
    nodes = nodes.sortBy(r => (r.start, r.end))
    maxLevel = index(nodes)
    map.clear()
    root = (1 << maxLevel) - 1
    rootStack = Array(maxLevel, root, 0)
    stack = Array.ofDim[Int](32,3)
  }

  override def put(start: Int, end: Int, value: V): V = {
    if (maxLevel > -1) {
      throw new IllegalStateException(s"Tried to add an interval [$start, $end] to already indexed tree")
    }
    map.get((start, end)) match {
      case Some(node) =>
        val v = node.getValue.get(0)
        node.addValue(value)
        v
      case _ =>
        val node = new Node[V](start, end)
        node.addValue(value)
        map.put((start, end), node)
        nodes += node
        value
    }
  }

  private case class StackCell(k: Int, x: Int, w: Boolean)

  override def overlappers(start: Int, end: Int): util.Iterator[BaseNode[V]] = {
    val result = new util.ArrayList[BaseNode[V]]()
    stack(0) = rootStack
    stack(0)(2) = 0 //reset processed root node
    var t = 1

    while (t >= 1) {
      t -= 1
      val z = stack(t)
      val zKDec = z(0) - 1
      if (z(0) <= 3) {
        val i0 = z(1) >> z(0) << z(0)
        var i1 = i0 + (1 << (z(0) + 1)) - 1

        if (i1 >= nodes.length) {
          i1 = nodes.length
        }

        var i = i0
        while (i < i1 && nodes(i).start <= end) {
          if (start <= nodes(i).end) {
            result.add(nodes(i))
          }
          i += 1
        }
      } else if (z(2) == 0) {
        val y = z(1) - (1 << zKDec) // the left child of z.x; NB: y may be out of range (i.e. y>=a.size())
        z(2) = 1 // re-add node z.x, but mark the left child having been processed
        t += 1
        if (y >= nodes.length || nodes(y).max >= start) { // push the left child if y is out of range or may overlap with the query
          val z = stack(t)
          z(0) = zKDec
          z(1) = y
          z(2) = 0
          t += 1
        }
      } else if (z(1) < nodes.length && nodes(z(1)).start <= end) { // need to push the right child
        if (start <= nodes(z(1)).end) {
          result.add(nodes(z(1)))
        } // test if z.x overlaps the query; if yes, append to out[]
        stack(t) = Array(zKDec, z(1) + (1 << zKDec), 0)// push the right child
        t += 1
      }
    }

    result.iterator()
  }


  override def find(start: Int, end: Int): Node[V] = {
    if (maxLevel == -1) {
      return nodes.find(p => p.start == start && p.end == end).orNull
    }
    val index = binarySearch(start, end)
    if (index == -1) {
      null
    } else {
      nodes(index)
    }
  }

  override def remove(start: Int, end: Int): V = {
    if (maxLevel > -1) {
      throw new IllegalStateException(s"Tried to delete an interval [$start, $end] from already indexed tree")
    }

    if (!map.contains((start, end))) {
      throw new IllegalArgumentException(s"Tried to delete a non-existing interval [$start, $end]")
    }

    val v = map((start, end))
    nodes.remove(nodes.indexWhere(node => node.start == start && node.end == end))

    v.getValue.get(0)
  }

  override def iterator(): java.util.Iterator[BaseNode[V]] = new StandardIterator()

  private def index(nodes: ArrayBuffer[Node[V]]): Int = {
    var i = 0L
    var last: Int = 0
    var last_i: Long = 0
    var k: Int = 1;
    while (i < nodes.length) {
      nodes(i.toInt).max = nodes(i.toInt).end
      last = nodes(i.toInt).max
      last_i = i
      i += 2
    }

    while ((1L << k) <= nodes.length) {
      val x = 1L << (k - 1)
      val i0 = (x << 1) - 1
      val step = x << 2
      i = i0
      while (i < nodes.length) {
        val leftMax = nodes((i - x).toInt).max
        val rightMax = if (i + x < nodes.length) nodes((i + x).toInt).max else last
        nodes(i.toInt).max = math.max(leftMax, rightMax).max(nodes(i.toInt).end)
        i += step
      }

      last_i = if (((last_i >> k) & 1) != 0) {
        last_i - x
      } else {
        last_i + x
      }
      if (last_i < nodes.length && nodes(last_i.toInt).max > last) {
        last = nodes(last_i.toInt).max
      }

      k += 1
    }

    k - 1
  }

  private def binarySearch(start: Int, end: Int): Int = {
    var s: Int = 0
    var e: Int = nodes.length - 1
    while (s <= e) {
      val mid = s + (e - s) / 2
      if (nodes(mid).start == start && nodes(mid).end == end) {
        return mid
      }
      if (nodes(mid).start > start || (nodes(mid).start == start && nodes(mid).end > end)) {
        e = mid - 1
      } else {
        s = mid + 1
      }
    }
    -1
  }

  private class StandardIterator extends java.util.Iterator[BaseNode[V]] {
    var i = 0
    override def hasNext: Boolean = i < nodes.length

    override def next(): BaseNode[V] = {
      val v = nodes(i)
      i += 1
      v
    }
  }


}
