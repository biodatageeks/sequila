package org.biodatageeks.sequila.rangejoins.methods.IntervalTree

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SequilaSession.logger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SizeEstimator
import org.biodatageeks.sequila.datasources.BAM.{BAMFileReader, BDGAlignFileReaderWriter}
import org.biodatageeks.sequila.rangejoins.IntervalTree.Interval
import org.biodatageeks.sequila.rangejoins.optimizer.{JoinOptimizerChromosome, RangeJoinMethod}
import org.openjdk.jol.info.GraphLayout
import org.seqdoop.hadoop_bam.BAMBDGInputFormat

import scala.collection.JavaConversions._

case class FeatureCountsPlan(spark: SparkSession,
                             readsPath: String,
                             genesPath: String,
                             output: Seq[Attribute],
                             minOverlap: Int,
                             maxGap: Int,
                             intervalHolderClassName: String)
  extends SparkPlan with Serializable with BDGAlignFileReaderWriter[BAMBDGInputFormat] {

  override protected def doExecute(): RDD[InternalRow] = {
    val reads = new BAMFileReader[BAMBDGInputFormat](spark, readsPath, None).readFile
    val genesRdd = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(genesPath)
      .rdd

    val optimizer = new JoinOptimizerChromosome(spark, genesRdd, genesRdd.count())
    logger.info(optimizer.debugInfo)

    if (optimizer.getRangeJoinMethod == RangeJoinMethod.JoinWithRowBroadcast) {
      val localIntervals = {
        if (maxGap != 0) {
          genesRdd
            .map(r => (r.getString(1), toInterval(r, maxGap), toInternalRow(r)))
        } else {
          genesRdd
            .map(r => (r.getString(1), toInterval(r), toInternalRow(r)))
        }
      }
        .collect()

      val intervalTree = {
        val tree = new IntervalHolderChromosome[InternalRow](localIntervals, intervalHolderClassName)
        try {
          val treeSize = GraphLayout.parseInstance(tree).totalSize()
          logger.info(s"Real broadcast size of the interval structure is ${treeSize} bytes")
        }
        catch {
          case e@(_: NoClassDefFoundError | _: ExceptionInInitializerError) =>
            logger.error("Cannot get broadcast size, method ObjectSizeCalculator.getObjectSize not available falling back to Spark method")
            val treeSize = SizeEstimator.estimate(tree)
            logger.info(s"Real broadcast size of the interval structure is ${treeSize} bytes")

        }
        spark.sparkContext.broadcast(tree)
      }

      val v3 = reads.mapPartitions(readIterator => {
        readIterator.map(read => {
          intervalTree.value.getIntervalTreeByChromosome(read.getContig) match {
            case Some(t) => {
              val record = t.overlappers(read.getStart, read.getEnd)
              if (minOverlap != 1) {
                record
                  .filter(r => calcOverlap(read.getStart, read.getEnd, r.getStart, r.getEnd) >= minOverlap)
                  .flatMap(k => k.getValue)
              } else {
                record.flatMap(k => k.getValue)
              }
            }
            case _ => Iterator.empty
          }
        })
      })
        .flatMap(r => r)
      v3
    } else {
      val genesRddWithIndex = genesRdd.zipWithIndex()
      val localIntervals = genesRddWithIndex
          .map(r=>(r._1.getString(1), toInterval(r._1) ,r._2))
          .collect()

      val intervalTree = {
        val tree = new IntervalHolderChromosome[Long](localIntervals, intervalHolderClassName)
        spark.sparkContext.broadcast(tree)
      }

      val v3 = reads.mapPartitions(readIterator => {
        readIterator.map(read => {
          intervalTree.value.getIntervalTreeByChromosome(read.getContig) match {
            case Some(t) => {
              val record = t.overlappers(read.getStart, read.getEnd)
              if (minOverlap != 1) {
                record
                  .filter(r => calcOverlap(read.getStart, read.getEnd, r.getStart, r.getEnd) >= minOverlap)
                  .flatMap(k => k.getValue.map(s => (s, s)))
              } else {
                record.flatMap(k => k.getValue.map(s => (s, s)))
              }
            }
            case _ => Iterator.empty
          }
        })
      })
        .flatMap(r => r)

      val intGenesRdd = genesRddWithIndex.map(r => (r._2, toInternalRow(r._1)))
      val result =  v3.
        join(intGenesRdd)
        .map(l => l._2._2)
      result
    }
  }

  private def toInternalRow(r: Row): InternalRow = {
    InternalRow.fromSeq(Seq(
      UTF8String.fromString(r.getString(0)), //Sample
      UTF8String.fromString(r.getString(1)), //Contig
      r.getString(2).toInt, //Start
      r.getString(3).toInt, //End
      UTF8String.fromString(r.getString(4)), //Strand
      r.getString(3).toInt - r.getString(2).toInt //Length
    ))
  }

  private def calcOverlap(start1: Int, end1: Int, start2: Int, end2: Int) = (math.min(end1, end2) - math.max(start1, start2) + 1)

  private def toInterval(r: Row) : Interval[Int] = {
    Interval(r.getString(2).toInt, r.getString(3).toInt)
  }

  private def toInterval(r: Row, maxGap: Int) : Interval[Int] = {
    Interval(r.getString(2).toInt - maxGap, r.getString(3).toInt + maxGap)
  }

  override def children: Seq[SparkPlan] = Nil
}
