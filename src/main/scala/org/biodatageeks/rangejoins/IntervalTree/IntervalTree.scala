/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.biodatageeks.rangejoins.IntervalTree

import org.apache.spark.sql.catalyst.InternalRow

class IntervalTree[T](allRegions: List[IntervalWithRow[Int]]) extends Serializable {

  val root = new Node(allRegions)

  def getAllOverlappings(r: IntervalWithRow[Int]) = allOverlappingRegions(r, root)

  private def allOverlappingRegions(r: IntervalWithRow[Int], rt: Node): List[(IntervalWithRow[Int])] = {
    if (rt == null) {
      return Nil
    }
    val resultFromThisNode = r match {
      case x if (rt.inclusiveIntervals == Nil) => Nil // Sometimes a node can have zero intervals
      // Save unnecessary filtering
      case x if (x.end < rt.minPointOfCollection || x.start > rt.maxPointOfCollection) => Nil
      case _ => rt.inclusiveIntervals.filter(t => r.overlaps(t))
    }
    if (r.overlaps(IntervalWithRow[Int](rt.centerPoint, rt.centerPoint + 1,r.row))) {
      return resultFromThisNode ++ allOverlappingRegions(r, rt.leftChild) ++
        allOverlappingRegions(r, rt.rightChild)
    }
    else if (r.end <= rt.centerPoint) {
      return resultFromThisNode ++ allOverlappingRegions(r, rt.leftChild)
    }
    else if (r.start > rt.centerPoint) {
      return resultFromThisNode ++ allOverlappingRegions(r, rt.rightChild)
    }
    else throw new NoSuchElementException("Interval Tree Exception. Illegal " +
      "comparison for centerpoint " + rt.centerPoint + " " + r.toString + " cmp: " +
      r.overlaps(IntervalWithRow[Int](rt.centerPoint, rt.centerPoint + 1,r.row)))
  }

  class Node(allRegions: List[IntervalWithRow[Int]]) extends Serializable {
    private val largestPoint = allRegions.maxBy(_.end).end
    private val smallestPoint = allRegions.minBy(_.start).start
    val centerPoint = smallestPoint + (largestPoint - smallestPoint) / 2
    val (inclusiveIntervals, leftChild, rightChild) = distributeRegions()
    val minPointOfCollection: Int = inclusiveIntervals match {
      case Nil => -1
      case _ => inclusiveIntervals.minBy(_.start).start
    }
    val maxPointOfCollection: Int = inclusiveIntervals match {
      case Nil => -1
      case _ => inclusiveIntervals.maxBy(_.end).end
    }

    def distributeRegions() = {
      var leftRegions: List[(IntervalWithRow[Int])] = Nil
      var rightRegions: List[(IntervalWithRow[Int])] = Nil
      var centerRegions: List[(IntervalWithRow[Int])] = Nil
      allRegions.foreach(x => {
        if (x.end < centerPoint) leftRegions ::= x
        else if (x.start > centerPoint) rightRegions ::= x
        else centerRegions ::= x
      })
      val leftChild: Node = leftRegions match {
        case Nil => null
        case _ => new Node(leftRegions)
      }
      val rightChild: Node = rightRegions match {
        case Nil => null
        case _ => new Node(rightRegions)
      }
      (centerRegions, leftChild, rightChild)
    }
  }

}

