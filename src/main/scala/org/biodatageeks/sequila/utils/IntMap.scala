package scala
package collection
package mutable


import scala.collection.generic.CanBuildFrom
import scala.collection.{GenTraversableOnce, mutable}
  

final class IntMap[V] private[collection] (defaultEntry: Int => V, initialBufferSize: Int, initBlank: Boolean)
  extends mutable.AbstractMap[Int, V]
    with Map[Int, V]
    with mutable.MapLike[Int, V, IntMap[V]]
    with Serializable
{
  import IntMap._

  def this() = this(IntMap.exceptionDefault, 16, true)

  /** Creates a new `IntMap` that returns default values according to a supplied key-value mapping. */
  def this(defaultEntry: Int => V) = this(defaultEntry, 16, true)

  /** Creates a new `IntMap` with an initial buffer of specified size.
    *
    *  A IntMap can typically contain half as many elements as its buffer size
    *  before it requires resizing.
    */
  def this(initialBufferSize: Int) = this(IntMap.exceptionDefault, initialBufferSize, true)

  /** Creates a new `IntMap` with specified default values and initial buffer size. */
  def this(defaultEntry: Int => V,  initialBufferSize: Int) = this(defaultEntry,  initialBufferSize,  true)

  private[this] var mask = 0
  private[this] var extraKeys: Int = 0
  private[this] var zeroValue: AnyRef = null
  private[this] var minValue: AnyRef = null
  private[this] var _size = 0
  private[this] var _vacant = 0
  private[this] var _keys: Array[Int] = null
  private[this] var _values: Array[AnyRef] = null

  if (initBlank) defaultInitialize(initialBufferSize)

  private[this] def defaultInitialize(n: Int) = {
    mask =
      if (n<0) 0x7
      else (((1 << (32 - java.lang.Integer.numberOfLeadingZeros(n-1))) - 1) & 0x3FFFFFFF) | 0x7
    _keys = new Array[Int](mask+1)
    _values = new Array[AnyRef](mask+1)
  }

  private[collection] def initializeTo(
                                        m: Int, ek: Int, zv: AnyRef, mv: AnyRef, sz: Int, vc: Int, kz: Array[Int], vz: Array[AnyRef]
                                      ) {
    mask = m; extraKeys = ek; zeroValue = zv; minValue = mv; _size = sz; _vacant = vc; _keys = kz; _values = vz
  }

  override def size: Int = _size + (extraKeys+1)/2
  override def empty: IntMap[V] = new IntMap()

  private def imbalanced: Boolean =
    (_size + _vacant) > 0.5*mask || _vacant > _size

  private def toIndex(k: Int): Int = {
    // Part of the MurmurHash3 32 bit finalizer
//    val h = (k ^ (k >>> 32))
    val x = (k ^ (k >>> 16)) * 0x85EBCA6B
    (x ^ (x >>> 13)) & mask
  }

  private def seekEmpty(k: Int): Int = {
    var e = toIndex(k)
    var x = 0
    while (_keys(e) != 0) { x += 1; e = (e + 2*(x+1)*x - 3) & mask }
    e
  }

  private def seekEntry(k: Int): Int = {
    var e = toIndex(k)
    var x = 0
    var q = 0
    while ({ q = _keys(e); if (q==k) return e; q != 0}) { x += 1; e = (e + 2*(x+1)*x - 3) & mask }
    e | MissingBit
  }

  private def seekEntryOrOpen(k: Int): Int = {
    var e = toIndex(k)
    var x = 0
    var q = 0
    while ({ q = _keys(e); if (q==k) return e; q+q != 0}) {
      x += 1
      e = (e + 2*(x+1)*x - 3) & mask
    }
    if (q == 0) return e | MissingBit
    val o = e | MissVacant
    while ({ q = _keys(e); if (q==k) return e; q != 0}) {
      x += 1
      e = (e + 2*(x+1)*x - 3) & mask
    }
    o
  }

  //done
  override def contains(key: Int): Boolean = {
    if (key == -key) (((key>>>31)+1) & extraKeys) != 0
    else seekEntry(key) >= 0
  }

  //done
  override def get(key: Int): Option[V] = {
    if (key == -key) {
      if ((((key>>>31)+1) & extraKeys) == 0) None
      else if (key == 0) Some(zeroValue.asInstanceOf[V])
      else Some(minValue.asInstanceOf[V])
    }
    else {
      val i = seekEntry(key)
      if (i < 0) None else Some(_values(i).asInstanceOf[V])
    }
  }

  override def getOrElse[V1 >: V](key: Int, default: => V1): V1 = {
    if (key == -key) {
      if ((((key>>>31)+1) & extraKeys) == 0) default
      else if (key == 0) zeroValue.asInstanceOf[V1]
      else minValue.asInstanceOf[V1]
    }
    else {
      val i = seekEntry(key)
      if (i < 0) default else _values(i).asInstanceOf[V1]
    }
  }
//done
  override def getOrElseUpdate(key: Int, defaultValue: => V): V = {
    if (key == -key) {
      val kbits = (key>>>31) + 1
      if ((kbits & extraKeys) == 0) {
        val value = defaultValue
        extraKeys |= kbits
        if (key == 0) zeroValue = value.asInstanceOf[AnyRef]
        else minValue = value.asInstanceOf[AnyRef]
        value
      }
      else if (key == 0) zeroValue.asInstanceOf[V]
      else minValue.asInstanceOf[V]
    }
    else {
      var i = seekEntryOrOpen(key)
      if (i < 0) {
        // It is possible that the default value computation was side-effecting
        // Our hash table may have resized or even contain what we want now
        // (but if it does, we'll replace it)
        val value = {
          val ok = _keys
          val ans = defaultValue
          if (ok ne _keys) {
            i = seekEntryOrOpen(key)
            if (i >= 0) _size -= 1
          }
          ans
        }
        _size += 1
        val j = i & IndexMask
        _keys(j) = key
        _values(j) = value.asInstanceOf[AnyRef]
        if ((i & VacantBit) != 0) _vacant -= 1
        else if (imbalanced) repack()
        value
      }
      else _values(i).asInstanceOf[V]
    }
  }

  //done
  /** Retrieves the value associated with a key, or the default for that type if none exists
    *  (null for AnyRef, 0 for floats and integers).
    *
    *  Note: this is the fastest way to retrieve a value that may or
    *  may not exist, if the default null/zero is acceptable.  For key/value
    *  pairs that do exist,  `apply` (i.e. `map(key)`) is equally fast.
    */
  def getOrNull(key: Int): V = {
    if (key == -key) {
      if ((((key>>>31)+1) & extraKeys) == 0) null.asInstanceOf[V]
      else if (key == 0) zeroValue.asInstanceOf[V]
      else minValue.asInstanceOf[V]
    }
    else {
      val i = seekEntry(key)
      if (i < 0) null.asInstanceOf[V] else _values(i).asInstanceOf[V]
    }
  }

  /** Retrieves the value associated with a key.
    *  If the key does not exist in the map, the `defaultEntry` for that key
    *  will be returned instead.
    */
  override def apply(key: Int): V = {
    if (key == -key) {
      if ((((key>>>31)+1) & extraKeys) == 0) defaultEntry(key)
      else if (key == 0) zeroValue.asInstanceOf[V]
      else minValue.asInstanceOf[V]
    }
    else {
      val i = seekEntry(key)
      if (i < 0) defaultEntry(key) else _values(i).asInstanceOf[V]
    }
  }

  /** The user-supplied default value for the key.  Throws an exception
    *  if no other default behavior was specified.
    */
  override def default(key: Int) = defaultEntry(key)

  private def repack(newMask: Int) {
    val ok = _keys
    val ov = _values
    mask = newMask
    _keys = new Array[Int](mask+1)
    _values = new Array[AnyRef](mask+1)
    _vacant = 0
    var i = 0
    while (i < ok.length) {
      val k = ok(i)
      if (k != -k) {
        val j = seekEmpty(k)
        _keys(j) = k
        _values(j) = ov(i)
      }
      i += 1
    }
  }
//FIXME
  /** Repacks the contents of this `IntMap` for maximum efficiency of lookup.
    *
    *  For maps that undergo a complex creation process with both addition and
    *  removal of keys, and then are used heavily with no further removal of
    *  elements, calling `repack` after the end of the creation can result in
    *  improved performance.  Repacking takes time proportional to the number
    *  of entries in the map.
    */
  def repack() {
    var m = mask
    if (_size + _vacant >= 0.5*mask && !(_vacant > 0.2*mask)) m = ((m << 1) + 1) & IndexMask
    while (m > 8 && 8*_size < m) m = m >>> 1
    repack(m)
  }

  override def put(key: Int, value: V): Option[V] = {
    if (key == -key) {
      if (key == 0) {
        val ans = if ((extraKeys&1) == 1) Some(zeroValue.asInstanceOf[V]) else None
        zeroValue = value.asInstanceOf[AnyRef]
        extraKeys |= 1
        ans
      }
      else {
        val ans = if ((extraKeys&2) == 1) Some(minValue.asInstanceOf[V]) else None
        minValue = value.asInstanceOf[AnyRef]
        extraKeys |= 2
        ans
      }
    }
    else {
      val i = seekEntryOrOpen(key)
      if (i < 0) {
        val j = i & IndexMask
        _keys(j) = key
        _values(j) = value.asInstanceOf[AnyRef]
        _size += 1
        if ((i & VacantBit) != 0) _vacant -= 1
        else if (imbalanced) repack()
        None
      }
      else {
        val ans = Some(_values(i).asInstanceOf[V])
        _keys(i) = key
        _values(i) = value.asInstanceOf[AnyRef]
        ans
      }
    }
  }

  /** Updates the map to include a new key-value pair.
    *
    *  This is the fastest way to add an entry to a `IntMap`.
    */
  override def update(key: Int, value: V): Unit = {
    if (key == -key) {
      if (key == 0) {
        zeroValue = value.asInstanceOf[AnyRef]
        extraKeys |= 1
      }
      else {
        minValue = value.asInstanceOf[AnyRef]
        extraKeys |= 2
      }
    }
    else {
      val i = seekEntryOrOpen(key)
      if (i < 0) {
        val j = i & IndexMask
        _keys(j) = key
        _values(j) = value.asInstanceOf[AnyRef]
        _size += 1
        if ((i & VacantBit) != 0) _vacant -= 1
        else if (imbalanced) repack()
      }
      else {
        _keys(i) = key
        _values(i) = value.asInstanceOf[AnyRef]
      }
    }
  }

  /** Adds a new key/value pair to this map and returns the map. */
  def +=(key: Int, value: V): this.type = { update(key, value); this }

  def +=(kv: (Int, V)): this.type = { update(kv._1, kv._2); this }

  def -=(key: Int): this.type = {
    if (key == -key) {
      if (key == 0) {
        extraKeys &= 0x2
        zeroValue = null
      }
      else {
        extraKeys &= 0x1
        minValue = null
      }
    }
    else {
      val i = seekEntry(key)
      if (i >= 0) {
        _size -= 1
        _vacant += 1
        _keys(i) = Int.MinValue
        _values(i) = null
      }
    }
    this
  }

  def iterator: Iterator[(Int, V)] = new Iterator[(Int, V)] {
    private[this] val kz = _keys
    private[this] val vz = _values

    private[this] var nextPair: (Int, V) =
      if (extraKeys==0) null
      else if ((extraKeys&1)==1) (0, zeroValue.asInstanceOf[V])
      else (Int.MinValue, minValue.asInstanceOf[V])

    private[this] var anotherPair: (Int, V) =
      if (extraKeys==3) (Int.MinValue, minValue.asInstanceOf[V])
      else null

    private[this] var index = 0

    def hasNext: Boolean = nextPair != null || (index < kz.length && {
      var q = kz(index)
      while (q == -q) {
        index += 1
        if (index >= kz.length) return false
        q = kz(index)
      }
      nextPair = (kz(index), vz(index).asInstanceOf[V])
      index += 1
      true
    })
    def next: (Int, V) = {
      if (nextPair == null && !hasNext) throw new NoSuchElementException("next")
      val ans = nextPair
      if (anotherPair != null) {
        nextPair = anotherPair
        anotherPair = null
      }
      else nextPair = null
      ans
    }
  }

  override def foreach[U](f: ((Int,V)) => U) {
    if ((extraKeys & 1) == 1) f((0, zeroValue.asInstanceOf[V]))
    if ((extraKeys & 2) == 2) f((Int.MinValue, minValue.asInstanceOf[V]))
    var i,j = 0
    while (i < _keys.length & j < _size) {
      val k = _keys(i)
      if (k != -k) {
        j += 1
        f((k, _values(i).asInstanceOf[V]))
      }
      i += 1
    }
  }

  override def clone(): IntMap[V] = {
    val kz = java.util.Arrays.copyOf(_keys, _keys.length)
    val vz = java.util.Arrays.copyOf(_values,  _values.length)
    val lm = new IntMap[V](defaultEntry, 1, false)
    lm.initializeTo(mask, extraKeys, zeroValue, minValue, _size, _vacant, kz,  vz)
    lm
  }

  override def +[V1 >: V](kv: (Int, V1)): IntMap[V1] = {
    val lm = clone().asInstanceOf[IntMap[V1]]
    lm += kv
    lm
  }

  override def ++[V1 >: V](xs: GenTraversableOnce[(Int, V1)]): IntMap[V1] = {
    val lm = clone().asInstanceOf[IntMap[V1]]
    xs.foreach(kv => lm += kv)
    lm
  }

  override def updated[V1 >: V](key: Int, value: V1): IntMap[V1] = {
    val lm = clone().asInstanceOf[IntMap[V1]]
    lm += (key, value)
    lm
  }

  /** Applies a function to all keys of this map. */
  def foreachKey[A](f: Int => A) {
    if ((extraKeys & 1) == 1) f(0)
    if ((extraKeys & 2) == 2) f(Int.MinValue)
    var i,j = 0
    while (i < _keys.length & j < _size) {
      val k = _keys(i)
      if (k != -k) {
        j += 1
        f(k)
      }
      i += 1
    }
  }

  /** Applies a function to all values of this map. */
  def foreachValue[A](f: V => A) {
    if ((extraKeys & 1) == 1) f(zeroValue.asInstanceOf[V])
    if ((extraKeys & 2) == 2) f(minValue.asInstanceOf[V])
    var i,j = 0
    while (i < _keys.length & j < _size) {
      val k = _keys(i)
      if (k != -k) {
        j += 1
        f(_values(i).asInstanceOf[V])
      }
      i += 1
    }
  }


}

object IntMap {
  private final val IndexMask  = 0x3FFFFFFF
  private final val MissingBit = 0x80000000
  private final val VacantBit  = 0x40000000
  private final val MissVacant = 0xC0000000

  private val exceptionDefault: Int => Nothing = (k: Int) => throw new NoSuchElementException(k.toString)

  implicit def canBuildFrom[V, U]: CanBuildFrom[IntMap[V], (Int, U), IntMap[U]] =
    new CanBuildFrom[IntMap[V], (Int, U), IntMap[U]] {
      def apply(from: IntMap[V]): IntMapBuilder[U] = apply()
      def apply(): IntMapBuilder[U] = new IntMapBuilder[U]
    }

  /** A builder for instances of `IntMap`.
    *
    *  This builder can be reused to create multiple instances.
    */
  final class IntMapBuilder[V] extends ReusableMapBuilder[(Int, V), IntMap[V]] {
    private[collection] var elems: IntMap[V] = new IntMap[V]
    def +=(entry: (Int, V)): this.type = {
      elems += entry
      this
    }
    def clear() { elems = new IntMap[V] }
    def result(): IntMap[V] = elems
  }

  /** Creates a new `IntMap` with zero or more key/value pairs. */
  def apply[V](elems: (Int, V)*): IntMap[V] = {
    val sz = if (elems.hasDefiniteSize) elems.size else 4
    val lm = new IntMap[V](sz * 2)
    elems.foreach{ case (k,v) => lm(k) = v }
    if (lm.size < (sz>>3)) lm.repack()
    lm
  }

  /** Creates a new empty `IntMap`. */
  def empty[V]: IntMap[V] = new IntMap[V]

  /** Creates a new empty `IntMap` with the supplied default */
  def withDefault[V](default: Int => V): IntMap[V] = new IntMap[V](default)

  /** Creates a new `LongMap` from arrays of keys and values.
    *  Equivalent to but more efficient than `LongMap((keys zip values): _*)`.
    */
  def fromZip[V](keys: Array[Int], values: Array[V]): IntMap[V] = {
    val sz = math.min(keys.length, values.length)
    val lm = new IntMap[V](sz * 2)
    var i = 0
    while (i < sz) { lm(keys(i)) = values(i); i += 1 }
    if (lm.size < (sz>>3)) lm.repack()
    lm
  }

  /** Creates a new `IntMap` from keys and values.
    *  Equivalent to but more efficient than `LongMap((keys zip values): _*)`.
    */
  def fromZip[V](keys: collection.Iterable[Int], values: collection.Iterable[V]): IntMap[V] = {
    val sz = math.min(keys.size, values.size)
    val lm = new IntMap[V](sz * 2)
    val ki = keys.iterator
    val vi = values.iterator
    while (ki.hasNext && vi.hasNext) lm(ki.next) = vi.next
    if (lm.size < (sz >> 3)) lm.repack()
    lm
  }
}

trait ReusableMapBuilder[-Elem, +To] extends mutable.Builder[Elem, To] {
  /** Clears the contents of this builder.
    *  After execution of this method, the builder will contain no elements.
    *
    *  If executed immediately after a call to `result`, this allows a new
    *  instance of the same type of collection to be built.
    */
  override def clear(): Unit    // Note: overriding for Scaladoc only!

  /** Produces a collection from the added elements.
    *
    *  After a call to `result`, the behavior of all other methods is undefined
    *  save for `clear`.  If `clear` is called, then the builder is reset and
    *  may be used to build another instance.
    *
    *  @return a collection containing the elements added to this builder.
    */
  override def result(): To    // Note: overriding for Scaladoc only!
}