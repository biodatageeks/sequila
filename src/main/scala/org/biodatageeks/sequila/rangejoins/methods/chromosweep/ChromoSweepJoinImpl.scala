package org.biodatageeks.sequila.rangejoins.methods.chromosweep

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{RangePartitioner, SparkContext}

object ChromoSweepJoinImpl extends Serializable {


  type K = SortedInterval[Int]
  type V = (Boolean, SortedInterval[Int], InternalRow)

  /**
   * Multi-joins together two RDDs that contain objects that map to reference regions.
   * The object from both sets will be joined using distributed version of the chromo-sweep algorithm.
   * The basic assumption is that all the intervals from specific range will be on one partition.
   * Process has following steps:
   * - create RangePartitioner from RDD1
   * - partition RDD1 using partitioner
   * - create range tree where the leaves are pairs of lowest starting and highest ending value of the intervals in a partition
   * - based on that, map RDD2 to split intervals that belong to multiple partitions and remove the ones that belong to none
   * - partition mapped RDD2 using previously created partitioner
   * - union both RDDs
   * - sort based on start value of the interval
   * - execute chromo-sweep on each partition.
   *
   * @param sc
   * @param rdd1 - first rdd to join (queries)
   * @param rdd2 - second rdd to join (labels)
   * @return
   */
  def overlapJoin(sc: SparkContext, rdd1: RDD[(SortedInterval[Int], InternalRow)], rdd2: RDD[(SortedInterval[Int], InternalRow)])
  : RDD[(InternalRow, InternalRow)] = {
    // partiton first RDD and based on get range of each partition
    val tRDD1 = rdd1.map(e => (e._1, (true, e._1, e._2)))
    val part = new RangePartitioner(tRDD1.getNumPartitions, tRDD1)
    val rangeTree = getRangeTree(new ShuffledRDD[K, V, V](tRDD1, part))
    // based on the ranges partition second RDD, split intervals that belong to more than one
    // remove the ones that belong to none
    val tRDD2 = rdd2.flatMap(e => rangeTree.getPartitions(e._1).map((_, (false, e._1, e._2))))
    val uRDD = new ShuffledRDD[K, V, V](tRDD1.union(tRDD2), part)
    // in each partition do chromosweep
    uRDD.mapPartitions { p =>
      val row = p.map(_._2).toList.sortBy(_._2.start).iterator
      val temp = new ChromoSweep()
      var i: V = null
      while (row.hasNext) {
        i = row.next()
        temp.next(i)
      }
      if (i._1 || temp.lastQuery.isDefined) {
        temp.push
      }
      // in case if there is no label for query we need to replace None with dummy internal row.
      val dummyInternalRow = createDummyInternalRow(temp)
      temp.mutList
        .map(e => (e.q.get._2, e.l.getOrElse((SortedInterval[Int](0, 0), dummyInternalRow))._2)).iterator
    }
  }

  private def createDummyInternalRow(temp: ChromoSweep) = {
    val ir = temp.mutList.head.q.get._2.copy()
    Range(0, ir.numFields).foreach(i => ir.setNullAt(i))
    ir
  }

  private def getRangeTree(rdd: RDD[(K, V)]) = {
    val arr = getPartitionRanges(rdd)
    new RangeTreeNode(arr)
  }

  /**
   * Calculates sum of all intervals for each partition
   * e.g. for parititon with intervals [1,3], [5,8], [11, 15] the result is [1,15]
   *
   * @param rdd
   * @return Array of sorted intervals for each parittion
   */
  private def getPartitionRanges(rdd: RDD[(K, V)]) = {
    rdd.mapPartitions(
      {
        p =>
          val temp = p.next()._1
          var max = temp.end
          var min = temp.start
          for (i <- p) {
            if (min > i._1.start) min = i._1.start
            if (max < i._1.end) max = i._1.end
          }
          (SortedInterval[Int](min, max) :: Nil).iterator
      }, true)
      .collect().sortBy(_.start)
  }
}

