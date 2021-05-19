package org.biodatageeks.sequila.rangejoins.methods.chromosweep

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable


/**
  * Contains all methods and fields necessary to perform chromo-sweep.
  */
class ChromoSweep extends Serializable {

  var lastQuery: Option[(SortedInterval[Int], InternalRow)] = None
  var lastLabel: Option[(SortedInterval[Int], InternalRow)] = None
  val mutList: mutable.MutableList[JoinedRow] = mutable.MutableList[JoinedRow]()
  // push value to result
  def push = mutList += JoinedRow(lastQuery, lastLabel)
  // update value of last label
  def updateLabel(i: (Boolean, SortedInterval[Int], InternalRow)):Unit = lastLabel = Some((i._2, i._3))
  // update value of last query
  def updateQuery(i: (Boolean, SortedInterval[Int], InternalRow)):Unit = lastQuery = Some((i._2, i._3))
  // clears value of label
  def flushLabel:Unit = lastLabel = None
  // clears value of query
  def flushQuery:Unit = lastQuery = None

  /**
    * Based on the state of the object and given element decides what action should be performed.
    * @param i
    */
  def next(i: (Boolean, SortedInterval[Int], InternalRow)):Unit = {
    (
      i._1, //if record is query (true) or a label (false)
      lastQuery.isDefined, // is there a query that we need to flush or push?
      lastLabel.isDefined, // is there a label that we need to flush or push?
      lastQuery.isDefined && i._2.start > lastQuery.get._1.end, // if start of current record is after end of last query
      lastLabel.isDefined && i._2.start > lastLabel.get._1.end) // if start of current record is after end of last label
    match {
      case (false, false, _, _, _) => updateLabel(i)
      case (false, true, false, true, _) => push; flushQuery; updateLabel(i)
      case (false, true, false, false, _) => updateLabel(i)
      case (false, _, true, _, false) => throw new IllegalStateException("Overlapping of two labels")
      case (false, true, true, false, true) => push; updateLabel(i)
      case (false, true, true, true, true) => push; flushQuery; updateLabel(i)
      case (true, false, false, _, _) => updateQuery(i)
      case (true, false, true, _, true) => flushLabel; updateQuery(i)
      case (true, false, true, _, false) => updateQuery(i)
      case (true, true, _, false, _) => throw new IllegalStateException("Overlapping of two queries")
      case (true, true, false, _, _) => push; updateQuery(i)
      case (true, true, true, true, false) => push; updateQuery(i)
      case (true, true, true, _, true) => push; flushLabel; updateQuery(i)
      case (a, b, c, d, e) => throw new IllegalStateException(s"unknown case: $a $b $c $d $e")
    }
  }
}


case class JoinedRow(q: Option[(SortedInterval[Int], InternalRow)], l: Option[(SortedInterval[Int], InternalRow)])