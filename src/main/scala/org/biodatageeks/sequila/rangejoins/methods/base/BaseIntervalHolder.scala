package org.biodatageeks.sequila.rangejoins.methods.base

import org.apache.log4j.Logger


trait BaseIntervalHolder[V] extends java.lang.Iterable[BaseNode[V] ] with Serializable {

  /**
    * Put a new interval into the tree (or update the value associated with an existing interval).
    * If the interval is novel, the special sentinel value is returned.
    *
    * @param start The interval's start.
    * @param end   The interval's end.
    * @param value The associated value.
    * @return The old value associated with that interval, or the sentinel.
    */
  def put( start: Int, end: Int, value: V): V

  /**
    * Remove an interval from the tree.  If the interval does not exist in the tree the
    * special sentinel value is returned.
    *
    * @param start The interval's start.
    * @param end   The interval's end.
    * @return The value associated with that interval, or the sentinel.
    */
  def remove( start: Int, end: Int): V

  /**
    * Find an interval.
    *
    * @param start The interval's start.
    * @param end   The interval's end.
    * @return The Node that represents that interval, or null.
    */
  def find(start: Int, end: Int) : BaseNode[V]

  /**
    * Return an iterator over all intervals overlapping the specified range.
    *
    * @param start The range start.
    * @param end   The range end.
    * @return An iterator.
    */
  def overlappers(start: Int, end: Int): java.util.Iterator[BaseNode[V]]

  /**
    * An optional method to include any struct actions required once all items are added
    */
  def postConstruct : Unit = {

  }
}
