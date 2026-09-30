package com.jnpersson.kmers.ry

import com.jnpersson.kmers.NTSeq
import com.jnpersson.kmers.minimizer.{CanonicalPriorities, MinTable, MinimizerPriorities}
import com.jnpersson.kmers.util.NTBitArray.empty
import com.jnpersson.kmers.util.{Arrays, NTBitArray}

object RYMinTable {
  def usingRaw(mers: Array[Int], width: Int): RYMinTable =
    RYMinTable(mers, width)

  def usingRawCanonical(mers: Array[Int], width: Int): CanonicalPriorities[RYMinTable] = {
    val inner = usingRaw(mers, width)
    val asCanonical =
      inner.byPriority.
        map(x => (NTBitArray.fromLong(x, inner.width).canonical &= inner.ryMask).toInt).
        distinct //keeps the first occurrence
    CanonicalPriorities(RYMinTable(asCanonical, width))
  }
}

/**
 * A lookup table of minimizers with relative priorities (minimizer ordering).
 * This is similar to [[MinTable]] except the RY alphabet is used to represent minimizers,
 * where R = purine (A/T) and Y = pyrimidine (C/T). Thus 1 bit per character, unlike the usual
 * 2 bit per character representation.
 *
 * @param byPriority Encoded minimizers (RY 2 bit encoding) ordered from high priority to low.
 *                   The position in the array is the rank of the corresponding minimizer.
 *                   All minimizers must be of equal length.
 * @param width minimizer width in characters
 * @param numLargeBuckets Number of buckets expected to be "very large", if any. Intended as a heuristic for
 *                        algorithms that may need to know this.
 */
final case class RYMinTable(byPriority: Array[Int], width: Int, override val numLargeBuckets: Long = 0)
  extends MinimizerPriorities {

  //Current limit with 2-bit encoding. Aiming to grow this limit when we use an 1 bit encoding in the future.
  assert (width <= 15)

  //Keep every other bit. AND with this bitmask converts ACGT 2 bit alphabet into
  //RY 2-bit (effectively A(00) and C(01).
  private[ry] val ryMask = NTBitArray.fill(0x5555555555555555L, width)

  //Number of bits required to store a priority
  private val priorityBits = {
    var r = 2
    var x = 4
    while (byPriority.length > x) {
      r += 2
      x *= 4
    }
    r
  }

  assert(width * 2 + priorityBits <= 64)

  //(4 ^ width)
  //This should eventually be a more efficient lookup table of smaller size (2^width with 1 bit encoding)
  private val maxMotifs = 1 << (width * 2)

  override val priorityWidth: Int =
    width + priorityBits / 2

  override def toString = s"RYMinTable(size=${byPriority.length}, width=$width)"

  @transient
  lazy val priorityLookup: Array[Int] = {
    val r = Arrays.fillNew(maxMotifs,-1)
    for ((motif, pri) <- byPriority.iterator.zipWithIndex) {
      r(motif) = pri
    }
    r
  }

  private final val pshift1 = width * 2

  /** Get the priority of the given minimizer, if any.
   * [[NTBitArray.empty]] indicates an invalid minimizer. */
  override def priorityOf(motif: NTBitArray): NTBitArray = {
    val p = priorityLookup((motif.clone() &= ryMask).toInt)

    if (p == -1) empty else {
      val prefixed = (p.toLong << pshift1) | motif.toLong
      NTBitArray.fromLong(prefixed, priorityWidth)
    }
  }

  /** Get the minimizer for a given priority. Inverse of the function above.
   * Only defined for values originally computed by the priorityOf function.
   */
  override def motifFor(priority: NTBitArray): NTBitArray =
    priority.cloneWithLength(width) <<= priorityBits

  private final val priorityShift = (32 - width) * 2 - priorityBits
  override def writePriorityOf(motif: NTBitArray, buffer: NTBitArray): NTBitArray = {
    val p = priorityLookup((motif.clone() &= ryMask).toInt)
    if (p == -1) empty
    else {
      val prefixed = (p.toLong << pshift1) | motif.toLong
      buffer.data(0) = prefixed << priorityShift
      buffer
    }
  }

  /** Total number of distinct minimizers in this ordering, if available. This may be an upper bound.
   * The largest value returned by priorityOf will be smaller than this value. */
  override def numMinimizers: Option[Long] =
    Some(1L << (width * 2 + priorityBits))

  /** Human-readable string corresponding to a given minimizer.
   * Not optimized for frequent use. */
//  override def humanReadable(priority: NTBitArray): NTSeq =
//    Representation2.decode(motifFor(priority))
}
