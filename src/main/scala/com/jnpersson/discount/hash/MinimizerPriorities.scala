/*
 * This file is part of Discount. Copyright (c) 2022 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.discount.hash

import com.jnpersson.discount.NTSeq
import com.jnpersson.discount.util.{BitRepresentation, NTBitArray}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  /**
   * Generate a motif table with all motifs of a certain length, in lexicographic order.
   * @param length the length
   * @param rna RNA mode (otherwise DNA will be used)
   * @return
   */
  def ofLength(length: Int, rna: Boolean = false): MinTable = using(motifsOfLength(length, rna).toSeq)

  /**
   * Create a motif table using the supplied minimizers in the given order.
   * @param mers Motifs in the desired priority order
   * @return The new motif table
   */
  def using(mers: Seq[NTSeq]) = new MinTable(mers.to(ArrayBuffer))

  /**
   * Create a new motif table from a template, preserving the relative ordering, but filtering
   * by the supplied valid set
   * @param template The template that should be filtered
   * @param validMers Motifs to be included. Others will be excluded.
   * @return A MinTable with filtered minimizer set
   */
  def filteredOrdering(template: MinTable, validMers: Iterable[NTSeq]): MinTable = {
    val validSet = validMers.to(mutable.Set)
    template.copy(byPriority = template.byPriority.filter(validSet.contains))
  }
}

/** Defines a reversible mapping between encoded minimizers and their priorities. */
trait MinimizerPriorities extends Serializable {
  /** Get the priority of the given minimizer.
  * If not every m-mer is a minimizer, then -1 indicates an invalid minimizer. */
  def priorityLookup(motif: Long): BucketId

  /** Get the minimizer for a given priority. Inverse of the function above. */
  def motifForPriority(priority: BucketId): Long

  /** Minimizer width (m) */
  def width: Int

  /** Total number of distinct minimizers in this ordering */
  def numMinimizers: Long

  /** Human-readable nucleotide sequence corresponding to a given minimizer */
  def humanReadable(priority: BucketId): NTSeq = s"$priority"

  /** Number of buckets (minimizers) expected to be very large (frequent), if any */
  def numLargeBuckets: Long = 0
}

/** Compute minimizer priority by XORing with a random mask */
final case class RandomXOR(width: Int, xorMask: Long, canonical: Boolean) extends MinimizerPriorities {
  //Long bitmask with the rightmost 2 * width bits set to 1
  private final val widthMask: Long = -1L >>> (64 - 2 * width)

  private final val adjustedMask = xorMask & widthMask

  override def priorityLookup(motif: Long): BucketId =
    if (canonical) { canonical(motif) ^ adjustedMask } else motif ^ adjustedMask

  // 4 ^ width
  override val numMinimizers: Long = 1 << (width * 2)

  override def motifForPriority(priority: BucketId): Long = priority ^ adjustedMask

  /** Find the canonical orientation of an encoded width-mer */
  private def canonical(motif: BucketId): BucketId = {
    val x = BitRepresentation.reverseComplement(motif, width, widthMask)
    if (motif < x) motif else x
  }
}


/**
 * A table of minimizers with relative priorities (minimizer ordering).
 * @param byPriority Minimizers ordered from high priority to low.
 *                   The position in the array is the rank, and also the unique ID in this table,
 *                   of the corresponding minimizer. All minimizers must be of equal length.
 * @param largeBuckets A subset of byPriority, indicating the motifs that have been found to correspond to
 *                     large buckets, if any.
 */
final case class MinTable(byPriority: ArrayBuffer[NTSeq], largeBuckets: ArrayBuffer[NTSeq] = ArrayBuffer.empty)
  extends MinimizerPriorities {

  if (largeBuckets.nonEmpty) {
    println(s"${largeBuckets.size} motifs are expected to generate large buckets.")
  }

  /** Minimizer width */
  val width: Int = byPriority.head.length

  override def numMinimizers: Long = byPriority.length.toLong
  override def humanReadable(priority: BucketId): NTSeq = byPriority(priority.toInt)

  override def numLargeBuckets: Long = largeBuckets.length

  /** A ShiftScanner associated with this MinTable (using its minimizer ordering) */
  @transient
  lazy val scanner: ShiftScanner = ShiftScanner(this)

  //4 ^ width
  private val maxMotifs = 1 << (width * 2)

  //bit shift distance
  private val shift = 64 - (width * 2)

  /** Compute the encoded form of a motif. Inefficient, not for frequent use.
   * Only works for widths up to 15 (30 bits).
   * Reversibly represents the motif as a 32-bit integer. This encoding is different from the position in the
   * byPriority array and independent of minimizer ordering. It depends only on the letters in the motif.
   */
  def encodedMotif(motif: NTSeq): Int = {
    val wrapped = NTBitArray.encode(motif)
    //We have generated a Long array, but only need a part of the first long in this case to give the final Int
    (wrapped.partAsLongArray(0, width)(0) >>> shift).toInt
  }

  override def priorityLookup(motif: Long): BucketId = priorityLookupArray(motif.toInt).toLong

  //Inefficient, not for frequent use
  override def motifForPriority(priority: BucketId): Long = encodedMotif(byPriority(priority.toInt))

  /**
   * Maps the bit-encoded integer form of each motif to its priority/rank.
   * priorityLookup always has size 4 &#94; width. Invalid entries will have priority -1.
   * Positions in the array correspond to the encoded form (see above), values correspond to the rank we use
   * (as used in the byPriority array), except for those set to -1.
   */
  val priorityLookupArray: Array[Int] = Array.fill(maxMotifs)(-1)
  for ((motif, pri) <- byPriority.iterator.zipWithIndex) {
    //Populate valid entries (other positions will remain set to -1)
    priorityLookupArray(encodedMotif(motif)) = pri
  }
}
