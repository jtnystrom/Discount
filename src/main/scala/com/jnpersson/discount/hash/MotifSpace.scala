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
  val all1mersDNA = Seq("A", "C", "G", "T")
  val all1mersRNA = Seq("A", "C", "G", "U")

  /**
   * Generate all sequences of the given length in lexicographic order.
   * @param length
   * @param rna
   * @return
   */
  def motifsOfLength(length: Int, rna: Boolean = false): Seq[String] = {
    val bases = if (rna) all1mersRNA else all1mersDNA
    if (length == 1) {
      bases
    } else if (length > 1) {
      motifsOfLength(length - 1, rna).flatMap(x => bases.iterator.map(y => x + y))
    } else {
      throw new Exception(s"Unsupported motif length $length")
    }
  }

  def ofLength(w: Int, rna: Boolean = false): MotifSpace = using(motifsOfLength(w, rna))

  def using(mers: Seq[String]) = new MotifSpace(mers.toArray)

  def fromTemplateWithValidSet(template: MotifSpace, validMers: Iterable[NTSeq]): MotifSpace = {
    val validSet = validMers.to[mutable.Set]
    template.copy(byPriority = template.byPriority.filter(validSet.contains))
  }
}

/**
 * A set of motifs that can be used, and their relative priorities (minimizer ordering).
 * @param byPriority Motifs in the space ordered from high priority to low.
 *                   The position in the array is the rank, and also the unique ID in this space,
 *                   of the corresponding minimizer.
 */
final case class MotifSpace(byPriority: Array[NTSeq]) {
  val width = byPriority.head.length

  @transient
  lazy val scanner = new ShiftScanner(this)

  def create(pattern: NTSeq, pos: Int): Motif = {
    Motif(pos, new Features(pattern, priorityOf(pattern), true))
  }

  //4 ^ width
  private val maxMotifs = 4 << (width * 2 - 2)

  //bit shift distance
  private val shift = 64 - (width * 2)

  /**
   * Compute the encoded form of a motif. Inefficient, not for frequent use.
   * Only works for widths up to 15 (30 bits).
   * Reversibly represents the motif as a 32-bit integer. This encoding is different from the position in the
   * byPriority array and independent of minimizer ordering. It depends only on the letters in the motif.
   * @param m
   * @return
   */
  def encodedMotif(m: NTSeq): Int = {
    val wrapped = NTBitArray.encode(m)
    //We have generated a Long array, but only need a part of the first long in this case to give the final Int
    (wrapped.partAsLongArray(0, width)(0) >>> shift).toInt
  }

  /**
   * Maps the bit-encoded integer form of each motif to its priority/rank.
   * priorityLookup always has size 4^width. Invalid entries will have priority -1.
   * Positions in the array correspond to the encoded form (see above), values correspond to the rank we use
   * (as used in the byPriority array), except for those set to -1.
   */

  //Initialize all positions to -1
  val priorityLookup: Array[Int] = Array.fill(maxMotifs)(-1)
  for ((motif, pri) <- byPriority.iterator.zipWithIndex) {
    //Populate with valid values
    priorityLookup(encodedMotif(motif)) = pri
  }

  /**
   * Inefficient way of obtaining the priority of a motif (priorityLookup is preferred)
   */
  def priorityOf(mk: NTSeq): Int =
    priorityLookup(encodedMotif(mk))
}