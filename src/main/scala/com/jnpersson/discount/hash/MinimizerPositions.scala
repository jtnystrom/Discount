/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
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

import com.jnpersson.discount.util.{KmerTable, NTBitArray}

/** This class adapts KmerTable for use as a list of minimizers of some underlying sequence.
 * This is more memory efficient than representing each NTBitArray as an object by itself.
 * Each entry in the KmerTable corresponds to a position in a sequence.
 * A single tag indicates whether the minimizer is valid or not.
 * @param data The minimizer data
 * @param width the length of each minimizer
 */
final class MinimizerPositions(data: KmerTable, width: Int) extends IndexedSeq[NTBitArray] {
  private val longsPerRecord = data.kmerWidth
  private val validTag = data.kmerWidth

  def isValid(position: Int): Boolean =
    data.kmers(validTag)(position) == 1

  def setValid(position: Int, flag: Boolean): Unit = {
    data.kmers(validTag)(position) = if (flag) 1 else 0
  }

  /** Obtain the minimizer for a given position. Allocates a new object. */
  override def apply(idx: Int): NTBitArray =
    new NTBitArray(data(idx), width)

  /** Get the minimizer at a position if it is valid, otherwise None */
  def get(position: Int): Option[NTBitArray] =
    if(isValid(position)) Some(this(position)) else None

  /** Lexicographic comparison of minimizers at two positions */
  def compare(pos1: Int, pos2: Int): Int = {
    var i = 0
    while (i < longsPerRecord) {
      val cmp = java.lang.Long.compareUnsigned(data.kmers(i)(pos1), data.kmers(i)(pos2))
      if (cmp != 0) return cmp
      i += 1
    }
    0
  }

  /** Lexicographic comparison of minimizers at two positions */
  def isBefore(pos1: Int, pos2: Int): Boolean =
    compare(pos1, pos2) < 0

  /** Lexicographic comparison of minimizers at two positions */
  def isAfter(pos1: Int, pos2: Int): Boolean =
    compare(pos1, pos2) > 0

  def length: Int = data.length

  /** Get all valid minimizers as an iterator (loses position information).
   * Expensive for long sequences.
   */
  def validBitArrayIterator: Iterator[NTBitArray] =
    Iterator.range(0, length).flatMap(get)

  /** Get all minimizers as a sequence, keeping position information.
   * Invalid positions will have the MinSplitter.INVALID constant.
   */
  def bitArraySeq: Seq[NTBitArray] =
    Iterator.range(0, length).map(get(_).getOrElse(MinSplitter.INVALID)).toSeq
}
