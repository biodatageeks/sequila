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

package org.biodatageeks.sequila.rangejoins.methods.genApp

import org.biodatageeks.sequila.rangejoins.genApp.{Interval, IntervalTree}

class IntervalTreeChromosome[T](allRegions: List[((String,Interval[Int]), T)]) extends Serializable {

  val intervalTreeHashMap:Map[String,IntervalTree[T]] = allRegions.groupBy(_._1._1).map(x => (x._1,new IntervalTree[T](x._2.map(y => (y._1._2,y._2)))))

  def getAllOverlappings(r: (String,Interval[Int])) = intervalTreeHashMap.get(r._1) match {
    case Some(t) => t.getAllOverlappings(r._2)
    case _ => Nil
  }

}