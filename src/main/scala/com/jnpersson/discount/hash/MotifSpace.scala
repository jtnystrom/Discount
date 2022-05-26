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
import com.jnpersson.discount.util.NTBitArray

import scala.collection.Seq
import scala.collection.mutable

object MotifSpace {
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
   * Generate a motif space with all motifs of a certain length, in lexicographic order.
   * @param length the length
   * @param rna RNA mode (otherwise DNA will be used)
   * @return
   */
  def ofLength(length: Int, rna: Boolean = false): MotifSpace = using(motifsOfLength(length, rna).toArray)

  /**
   * Create a motif space using the supplied minimizers in the given order.
   * @param mers Motifs in the desired priority order
   * @return The new motif space
   */
  def using(mers: Array[NTSeq]) = new MotifSpace(mers)

  /**
   * Create a new motif space from a template, preserving the relative ordering, but filtering
   * by the supplied valid set
   * @param template The template that should be filtered
   * @param validMers Motifs to be included. Others will be excluded.
   * @return A MotifSpace with filtered minimizer set
   */
  def fromTemplateWithValidSet(template: MotifSpace, validMers: Iterable[NTSeq]): MotifSpace = {
    val validSet = validMers.to(mutable.Set)
    template.copy(byPriority = template.byPriority.filter(validSet.contains))
  }
}

/**
 * A set of motifs that can be used for super-mer parsing, and their relative priorities (minimizer ordering).
 * @param byPriority Motifs in the space ordered from high priority to low.
 *                   Must be non-empty.
 *                   The position in the array is the rank, and also the unique ID in this space,
 *                   of the corresponding minimizer. Motifs must be of equal length.
 * @param largeBuckets A subset of byPriority, indicating the motifs that have been found to correspond to
 *                     large buckets.
 */
final case class MotifSpace(byPriority: Array[NTSeq], largeBuckets: Array[NTSeq] = Array()) {
  /** Minimizer width */
  val width: Int = byPriority.head.length

  /** A ShiftScanner associated with this MotifSpace (using its minimizer ordering) */
  @transient
  lazy val scanner: ShiftScanner = ShiftScanner(this)

  //4 ^ width
  private val maxMotifs = 4 << (width * 2 - 2)

  //bit shift distance
  private val shift = 64 - (width * 2)

  /**
   * Compute the encoded form of a motif. Inefficient, not for frequent use.
   * Only works for widths up to 15 (30 bits).
   * Reversibly represents the motif as a 32-bit integer. This encoding is different from the position in the
   * byPriority array and independent of minimizer ordering. It depends only on the letters in the motif.
   * @param motif motif to encode
   * @return
   */
  def encodedMotif(motif: NTSeq): Int = {
    val wrapped = NTBitArray.encode(motif)
    //We have generated a Long array, but only need a part of the first long in this case to give the final Int
    (wrapped.partAsLongArray(0, width)(0) >>> shift).toInt
  }

  /**
   * Maps the bit-encoded integer form of each motif to its priority/rank.
   * priorityLookup always has size 4 &#94; width. Invalid entries will have priority -1.
   * Positions in the array correspond to the encoded form (see above), values correspond to the rank we use
   * (as used in the byPriority array), except for those set to -1.
   */
  val priorityLookup: Array[Int] = Array.fill(maxMotifs)(-1)
  for ((motif, pri) <- byPriority.iterator.zipWithIndex) {
    //Populate valid entries (other positions will remain set to -1)
    priorityLookup(encodedMotif(motif)) = pri
  }
}