package genApp

case class Interval[T <% Int](start: T, end: T) {
  def overlaps(other: Interval[T]): Boolean = {
    (end >= start) && (other.end >= other.start) &&
      (end >= other.start && start <= other.end)
  }
}