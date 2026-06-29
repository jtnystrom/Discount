/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers.minimizer

import com.jnpersson.kmers._
import com.jnpersson.kmers.util.{Arrays, NTBitArray}

import scala.collection.mutable

object MinTable {
  val all1mersDNA = List("A", "C", "G", "T")
  val all1mersRNA = List("A", "C", "G", "U")

  /**
   * Generate all motifs of a certain length, in lexicographic order.
   * @param length The length
   * @param rna RNA mode (otherwise DNA will be used)
   */
  def motifsOfLength(length: Int, rna: Boolean = false): Iterator[NTSeq] = {
    val bases = if (rna) all1mersRNA else all1mersDNA
    if (length == 1) {
      bases.iterator
    } else if (length > 1) {
      motifsOfLength(length - 1, rna).flatMap(x => bases.iterator.map(y => x + y))
    } else {
      throw new Exception(s"Unsupported motif length $length")
    }
  }

  def encodedMotifsOfLength(length: Int): Iterator[NTBitArray] =
    Iterator.range(0, 1 << (2 * length)).map(x => NTBitArray.fromLong(x, length))

  /** Generate a motif table with all motifs of a certain length, in lexicographic order. */
  def ofLength(length: Int): MinTable =
    withAll(length)

  /**
   * Create a motif table using the supplied minimizers in the given order.
   * @param mers Motifs in the desired priority order
   * @return The new motif table
   */
  def using(mers: Seq[NTBitArray]): MinTable =
    MinTable(mers.iterator.map(_.toInt).toArray, mers.headOption.map(_.size).getOrElse(0))

  def usingRaw(mers: Array[Int], width: Int): MinTable =
    MinTable(mers, width)

  def withAll(width: Int): MinTable =
    MinTable(Array.range(0, 1 << (2 * width)), width)

  /**
   * Create a new motif table from a template, preserving the relative ordering, but filtering
   * by the supplied valid set
   * @param template The template that should be filtered
   * @param validMers Motifs to be included. Others will be excluded.
   * @return A MinTable with filtered minimizer set
   */
  def filteredOrdering(template: MinTable, validMers: Iterable[Int]): MinTable = {
    val validSet = mutable.BitSet.empty ++ validMers
    template.copy(byPriority = template.byPriority.filter(validSet.contains))
  }
}

/** Defines a bidirectional mapping between encoded minimizers and their priorities.
 * Priorities and minimizers are both represented as NTBitArray objects. If the bit array is
 * a priority, the first 2*width bits should be interpreted as an unsigned integer, where a smaller value
 * indicates a higher priority minimizer.
 * If the bit array is a minimizer, then it can be interpreted as a 2-bit encoded string of nucleotides in the usual
 * way.
 *
 * The unsigned integer space representing priorities is no larger than the number of m-mers,
 * so e.g. for nucleotides, for width m there is up to 4<sup>m</sup> possible minimizers and no more than 4<sup>m</sup>
 * priorities.
 *
 * The forward mapping (priorityOf) may be many-to-one, and not necessarily reversible. In this case, the
 * reverse mapping (motifFor) returns a representative minimizer.
 */
trait MinimizerPriorities extends Serializable {

  /** Get the priority of the given minimizer.
  * If not every m-mer is a minimizer, then [[NTBitArray.empty]] indicates an invalid minimizer. */
  def priorityOf(motif: NTBitArray): NTBitArray

  /** Write the priority of the given minimizer to the provided buffer.
   * If not every m-mer is a minimizer, then [[NTBitArray.empty]] indicates an invalid minimizer.
   * @param motif The motif to get the priority of
   * @param buffer The buffer to write the priority to
   * @return The buffer for chaining
   */
  def writePriorityOf(motif: NTBitArray, buffer: NTBitArray): NTBitArray = {
    import NTBitArray.empty
    val p = priorityOf(motif)
    if (p ne empty) {
      System.arraycopy(p.data, 0, buffer.data, 0, buffer.data.size)
      buffer
    } else empty
  }

  /** Get the minimizer for a given priority. Inverse of the function above.
   * Only defined for values originally computed by the priorityOf function.
   */
  def motifFor(priority: NTBitArray): NTBitArray

  /** Minimizer width (m) */
  def width: Int

  /** Total number of distinct minimizers in this ordering, if available. This may be an upper bound.
   * The largest value returned by priorityOf will be smaller than this value. */
  def numMinimizers: Option[Long] =
    if (width <= 30) Some(1L << (width * 2)) else None

  //Thread local decoder to let humanReadable be thread safe
  @transient protected lazy val decoder =
    ThreadLocal.withInitial(() => NTBitArray.decoder)

  def humanReadable(priority: Long): NTSeq =
    humanReadable(NTBitArray.fromLong(priority, width))

  /** Human-readable string corresponding to a given minimizer.
   * Not optimized for frequent use. */
  def humanReadable(priority: NTBitArray): NTSeq =
    decoder.get.longsToString(motifFor(priority).data, 0, width)

  /** Number of buckets (minimizers) expected to be very large (frequent), if any */
  def numLargeBuckets: Long = 0
}

/** Compute minimizer priority by XORing with a random mask */
final case class RandomXOR(width: Int, xorMask: Long, canonical: Boolean) extends MinimizerPriorities {

  val mask = {
    val buf = NTBitArray.longBuffer(width)
    var i = 0
    val lmerMask = -1L << ((32 - width % 32) * 2)
    while (i < buf.length) {
      if (i == buf.length - 1 && (width % 32 != 0)) {
        //to align with the end of the left-adjusted data. This mimics Kraken 2 behaviour
        buf(i) = xorMask << (64 - (width % 32) * 2)
      } else {
        buf(i) = xorMask
      }
      i += 1
    }
    NTBitArray(buf, width)
  }

  override def priorityOf(motif: NTBitArray): NTBitArray =
    if (canonical) { motif.canonical ^= mask } else motif.clone() ^= mask

  override def writePriorityOf(motif: NTBitArray, buffer: NTBitArray): NTBitArray = {
    assert(buffer.size == width, "Buffer size must match width")
    if (canonical) {
      motif.writeCanonical(buffer)
      buffer ^= mask
    } else {
      System.arraycopy(motif.data, 0, buffer.data, 0, motif.data.length)
      buffer ^= mask
    }
    buffer
  }

  override def motifFor(priority: NTBitArray): NTBitArray = priority.clone() ^= mask

}

/**
 * A lookup table of minimizers with relative priorities (minimizer ordering).
 * This can represent any ordering. The downside is that the entire table must be stored in memory,
 * so there is a natural limit to how wide m can be.
 *
 * @param byPriority Encoded minimizers ordered from high priority to low.
 *                   The position in the array is the rank of the corresponding minimizer.
 *                   All minimizers must be of equal length.
 * @param numLargeBuckets Number of buckets expected to be "very large", if any. Intended as a heuristic for
 *                        algorithms that may need to know this.
 */
final case class MinTable(byPriority: Array[Int], width: Int, override val numLargeBuckets: Long = 0)
  extends MinimizerPriorities {

  import NTBitArray.empty

  /**
   * Maps the bit-encoded integer form of each motif to its priority/rank. (This is the inverse of
   * [[byPriority]] above).
   * priorityLookup always has size 4 &#94; width.
   * Positions in the array correspond to the encoded form (see above), values correspond to the rank we use
   * (as used in the byPriority array). Invalid entries will be set to -1.
   */
  @transient
  lazy val priorityLookup: Array[Int] = {
    val r = Arrays.fillNew(maxMotifs,-1)
    for ((motif, pri) <- byPriority.iterator.zipWithIndex) {
      r(motif) = NTBitArray.fromLong(pri, width).toInt
    }
    r
  }

  //MinTable needs to represent motifs as signed Int, since they will be used as an array index.
  //This only works up to 15 (30 bits), 16 bp would need 32 bits.
  assert (width <= 15)

  if (numLargeBuckets > 0) {
    println(s"$numLargeBuckets motifs are expected to generate large buckets.")
  }

  override def toString = s"MinTable(size=${byPriority.length}, width=$width)"

  override def numMinimizers: Option[Long] = Some(byPriority.length.toLong)

  override def humanReadable(priority: NTBitArray): NTSeq =
    decoder.get.toString(motifFor(priority))

  /** A ShiftScanner associated with this MinTable (using its minimizer ordering) */
  @transient
  lazy val scanner: ShiftScanner = ShiftScanner(this)

  //4 ^ width
  private val maxMotifs = 1 << (width * 2)

  /** Encoded motifs as NTBitArray objects (new objects allocated on the fly) in priority order */
  def motifArrays: Array[NTBitArray] =
    motifArraysIterator.toArray

  /** Encoded motifs as NTBitArray objects (new objects allocated on the fly) in priority order */
  def motifArraysIterator: Iterator[NTBitArray] =
    byPriority.iterator.map(x => NTBitArray.fromLong(x, width))

  /** Motifs as human-readable strings in priority order */
  def humanReadableIterator: Iterator[NTSeq] =
    motifArraysIterator.map(decoder.get.toString)

  /** Encoded motif as a NTBitArray (a new object will be allocated) */
  def motifArray(priority: Int): NTBitArray = {
    val p = byPriority(priority)
    if (p == -1) empty else NTBitArray.fromLong(p, width)
  }

  override def priorityOf(motif: NTBitArray): NTBitArray = {
    val p = priorityLookup(motif.toInt)
    if (p == -1) empty else NTBitArray.fromLong(p, width)
  }

  override def writePriorityOf(motif: NTBitArray, buffer: NTBitArray): NTBitArray = {
    assert(buffer.size == width, "Buffer size must match width")
    val p = priorityLookup(motif.toInt)
    if (p == -1) empty
    else {
      val shift = (32 - width) * 2
      buffer.data(0) = p.toLong << shift
      buffer
    }
  }

  override def motifFor(priority: NTBitArray): NTBitArray =
    NTBitArray.fromLong(byPriority(priority.toInt), width)
}


/** Apply spaced seed mask to some minimizer priorities
 * s nts, 1 nt apart from each other, counting from the rightmost position, will be removed,
 * yielding (width-s) nts. Masked positions will be set to A
 * Example: TTCTGTGGG with s = 3 will be masked as TTC-G-G-G, yielding TTCAGAGAG
 *
 * @param s     the number of nts to mask out
 * @param inner inner priorities to mask
 */
final case class SpacedSeed(s: Int, inner: MinimizerPriorities) extends MinimizerPriorities {

  val width = inner.width
  assert (s <= inner.width / 2)

  final val spaceMask: NTBitArray = {

    //Set all bits
    val r = NTBitArray.fill(-1, width)

    var i = 0
    val finalBits = 3L << (64 - (width % 32) * 2)
    while (i < s) {
      r <<= 4 //clear 4 bits
      r.data(r.data.length - 1) = r.data(r.data.length - 1) | finalBits //set 2 bits
      i += 1
    }
    r
  }

  def maskSpacesOnly(motif: NTBitArray): NTBitArray =
    motif &= spaceMask

  override def priorityOf(motif: NTBitArray): NTBitArray =
    inner.priorityOf(motif) &= spaceMask

  override def writePriorityOf(motif: NTBitArray, buffer: NTBitArray): NTBitArray = {
    inner.writePriorityOf(motif, buffer)
    buffer &= spaceMask
    buffer
  }

  /** Not a true reverse function of priorityOf, since that loses information. We return a representative
   * value. */
  override def motifFor(priority: NTBitArray): NTBitArray =
    inner.motifFor(priority)

  override def numMinimizers: Option[Long] =
    inner.numMinimizers
}
