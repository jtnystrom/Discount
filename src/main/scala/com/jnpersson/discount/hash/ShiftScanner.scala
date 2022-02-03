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
import com.jnpersson.discount.util.{InvalidNucleotideException, ZeroNTBitArray}
import com.jnpersson.discount.util.BitRepresentation._


/**
 * Bit-shift scanner for fixed width motifs. Identifies all valid (according to some [[MotifSpace]])
 * motifs in a sequence.
 *
 * @param space The space to scan for motifs of
 */
final case class ShiftScanner(space: MotifSpace) {

  assert(space.width <= 15)

  private val width: Int = space.width

  //Int bitmask with the rightmost 2 * width bits set to 1
  private val mask: Int = {
    var r = 0
    var i = 0
    while (i < width) {
      r = (r << 2) | 3
      i += 1
    }
    r
  }

  /**
   * For each valid motif rank in the byPriority array, compute a corresponding
   * Features object that can be reused every time we encounter motifs with that rank (ID).
   */
  val featuresByPriority: Array[Features] =
    space.byPriority.zipWithIndex.map(p => Features(p._1, p._2, true))

  /**
   * Find all matches in the string, and encode super-mers.
   * Returns a pair of 1) the encoded nucleotide string,
   * 2) an array with the IDs (rank values) of matches (potential minimizers) in order, or Motif.INVALID for positions
   * where no valid matches were found. The first (m-1) items are always Motif.INVALID, so that
   * the position in the array corresponds to a position in the string.
   */
  def allMatches(data: NTSeq): (ZeroNTBitArray, Array[Int]) = {
    var writeLong = 0
    val longs = if (data.size % 32 == 0) { data.size / 32 } else { data.size / 32 + 1 }
    val encoded = new Array[Long](longs)
    var thisLong = 0L
    val r = Array.fill(data.length)(Motif.INVALID)
    try {
      var pos = 0
      var window: Int = 0
      while ((pos < width - 1) && pos < data.length) {
        val x = charToTwobit(data.charAt(pos))
        window = (window << 2) | x
        thisLong = (thisLong << 2) | x
        pos += 1
        //assume pos will not hit 32 in this loop
      }
      while (pos < data.length) {
        val x = charToTwobit(data.charAt(pos))
        window = ((window << 2) | x) & mask
        thisLong = (thisLong << 2) | x
        //window will now correspond to the "encoded form" of a motif (reversible mapping to 32-bit Int)
        //priorityLookup will give the rank/ID
        val priority = space.priorityLookup(window)
        r(pos) = priority
        pos += 1
        if (pos % 32 == 0) {
          encoded(writeLong) = thisLong
          writeLong += 1
          thisLong = 0L
        }
      }

      //left-adjust the bits inside the long array
      if (data.length > 0 && data.length % 32 != 0) {
        val finalShift = (32 - (data.length % 32)) * 2
        encoded(writeLong) = thisLong << finalShift
      }

      (ZeroNTBitArray(encoded, data.length), r)
    } catch {
      case ine: InvalidNucleotideException =>
        Console.err.println(
          s"Unable to parse sequence: '$data' because of character '${ine.invalidChar}' ${ine.invalidChar.toInt}")
        throw ine
    }
  }

  /**
   * Count motifs in a read and add them to the supplied MotifCounter.
   *
   * @param counter
   * @param read
   */
  def countMotifs(counter: MotifCounter, read: NTSeq) {
    for {m <- allMatches(read)._2; if m != Motif.INVALID} {
      counter increment m
    }
  }

  def countMotifs(counter: MotifCounter, reads: TraversableOnce[NTSeq]) {
    for (r <- reads) countMotifs(counter, r)
  }
}
