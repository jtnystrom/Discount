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
import com.jnpersson.discount.util.{Arrays, BitRepresentation, InvalidNucleotideException, ZeroNTBitArray}
import com.jnpersson.discount.util.BitRepresentation._

/**
 * Bit-shift scanner for fixed width motifs. Identifies all valid (according to some [[MinimizerPriorities]])
 * motifs/minimizers in a sequence.
 *
 * @param priorities The minimizer ordering to scan for motifs of
 */
final case class ShiftScanner(priorities: MinimizerPriorities) {

  private val width: Int = priorities.width

  //Long bitmask with the rightmost 2 * width bits set to 1
  private val mask: Long = -1L >>> (64 - 2 * width)

  /**
   * Find all matches in a nucleotide string.
   * @param data input data (NT sequence)
   * @return a pair of (encoded nucleotide string, minimizer IDs)
   */
  def allMatches(data: NTSeq): (ZeroNTBitArray, Array[Long]) =
    allMatches(i => charToTwobit(data.charAt(i)), data.length)

  /**
   * Find all matches in an encoded nucleotide string, or of its reverse complement.
   * @param data the encoded nucleotide string to find minimizers in
   * @param reverseComplement whether to traverse the RC of the string rather than the forward orientation
   * @return a pair of (encoded nucleotide string, minimizer IDs)
   */
  def allMatches(data: ZeroNTBitArray, reverseComplement: Boolean = false): (ZeroNTBitArray, Array[Long]) = {
    if (reverseComplement) {
      val max = data.size - 1
      allMatches(i => BitRepresentation.complementOne(data.apply(max - i)).toByte, data.size)
    } else {
      allMatches(i => data.apply(i), data.size)
    }
  }

  /**
   * Find all matches in a nucleotide string.
   * Returns a pair of 1) the encoded nucleotide string,
   * 2) an array with the IDs (rank values) of matches (potential minimizers) in order, or Motif.INVALID for positions
   * where no valid matches were found. The first (m-1) items are always Motif.INVALID, so that
   * the position in the array corresponds to a position in the string.
   *
   * @param data Function to get the two-bit encoded nucleotide at the given position [0, size)
   * @param size Length of input
   * @return a pair of (encoded nucleotide string, minimizer IDs)
   */
  def allMatches(data: Int => Byte, size: Int): (ZeroNTBitArray, Array[Long]) = {
    var writeLong = 0
    val longs = if (size % 32 == 0) { size / 32 } else { size / 32 + 1 }
    val encoded = new Array[Long](longs)
    var thisLong = 0L

    val r = Arrays.fillNew[Long](size, MinSplitter.INVALID)
    try {
      var pos = 0
      var window: Long = 0
      while ((pos < width - 1) && pos < size) {
        val x = data(pos)
        window = (window << 2) | x
        thisLong = (thisLong << 2) | x
        pos += 1
        //assume pos will not hit 32 in this loop
      }
      while (pos < size) {
        val x = data(pos)
        window = ((window << 2) | x) & mask
        thisLong = (thisLong << 2) | x
        //window will now correspond to the "encoded form" of a motif (reversible mapping to 32-bit Int)
        //priorityLookup will give the rank/ID
        val priority = priorities.priorityLookup(window)
        r(pos) = priority
        pos += 1
        if (pos % 32 == 0) {
          encoded(writeLong) = thisLong
          writeLong += 1
          thisLong = 0L
        }
      }

      //left-adjust the bits inside the long array
      if (size > 0 && size % 32 != 0) {
        val finalShift = (32 - (size % 32)) * 2
        encoded(writeLong) = thisLong << finalShift
      }

      (ZeroNTBitArray(encoded, size), r)
    } catch {
      case ine: InvalidNucleotideException =>
        Console.err.println(
          s"Unable to parse sequence: '$data' because of character '${ine.invalidChar}' ${ine.invalidChar.toInt}")
        throw ine
    }
  }
}
