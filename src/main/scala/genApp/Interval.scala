package genApp

case class Interval[T <% Long](start: T, end: T) {
  def overlaps(other: Interval[T]): Boolean = {
    (end >= start) && (other.end >= other.start) &&
      (end >= other.start && start <= other.end)
  }
}