/**
  * Licensed to Big Data Genomics (BDG) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The BDG licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package genApp

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, DataType}
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}

object Main {
  case class RecordData1(start1: Long, end1: Long) extends Serializable
  case class RecordData2(start2: Long, end2: Long) extends Serializable
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("ExtraStrategiesGenApp")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")
    spark.experimental.extraStrategies = new IntervalTreeJoinStrategy(spark) :: Nil


    var rdd1 = sc.parallelize(Seq((100L, 199L),
      (200L, 299L),
      (400L, 600L),
      (10000L, 20000L)))
      .map(i => Row(i._1, i._2))
    var rdd2 = sc.parallelize(Seq((150L, 250L),
      (300L, 500L),
      (500L, 700L),
      (22000L, 22300L)))
      .map(i => Row(i._1, i._2))

    val schema1 = StructType(Seq(StructField("start1", LongType), StructField("end1", LongType)))
    val schema2 = StructType(Seq(StructField("start2", LongType), StructField("end2", LongType)))

    var ds1 = spark.createDataFrame(rdd1,schema1)
      ds1.createOrReplaceTempView("s1")
    var ds2 = spark.createDataFrame(rdd2,schema2)
      ds2.createOrReplaceTempView("s2")

    println("select * from s1 JOIN s2 on " + "start1 < end1 and start2 < end2 and start1 < start2 and start2 < end1")
    spark.sql("select * from s1 JOIN s2 on " + "start1 < end1 and start2 < end2 and start1 < start2 and start2 < end1").explain
    spark.sql("select * from s1 JOIN s2 on " + "start1 < end1 and start2 < end2 and start1 < start2 and start2 < end1").show
    /*(100L, 199L, 150L, 250L) ::
      (200L, 299L, 150L, 250L) ::
      (400L, 600L, 300L, 500L) ::
      (400L, 600L, 500L, 700L) :: Nil*/
    //)
  }
}