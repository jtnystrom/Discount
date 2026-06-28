/*
 * This file is part of Discount. Copyright (c) 2019-2024 Johan Nyström-Persson.
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

package com.jnpersson.kmers.minimizer

import com.jnpersson.kmers.NTSeq
import com.jnpersson.kmers.util.BitRepresentation._
import com.jnpersson.kmers.util.{BitRepresentation, InvalidNucleotideException, KmerTable, NTBitArray}

/**
 * Bit-shift scanner for fixed width motifs. Identifies all valid (according to some [[MinimizerPriorities]])
 * motifs/minimizers in a sequence.
 *
 * @param priorities The minimizer ordering to scan for motifs of
 */
final case class ShiftScanner(priorities: MinimizerPriorities) {

  private val width: Int = priorities.width
  /**
   * Find all matches in a nucleotide string.
   * @param data input data (NT sequence)
   * @return a pair of (encoded nucleotide string, minimizer IDs)
   */
  def allMatches(data: NTSeq): (NTBitArray, MinimizerPositions) = {
    try {
      allMatches(i => charToTwobit(data.charAt(i)), data.length)
    } catch {
      case ine: InvalidNucleotideException =>
        Console.err.println(
          s"Unable to parse sequence: '$data' because of character '${ine.invalidChar}' ${ine.invalidChar.toInt}")
        throw ine
    }
  }

  /**
   * Efficiently find valid matches only in a nucleotide string. Invalid bases will be skipped correctly
   * as if the string was split.
   */
  def matchesOnly(data: NTSeq): Iterator[NTBitArray] =
    matchesOnly(i => charToTwobitWithInvalid(data.charAt(i)), data.length)

  /**
   * Find all matches in an encoded nucleotide string, or of its reverse complement.
   * @param data the encoded nucleotide string to find minimizers in
   * @param reverseComplement whether to traverse the RC of the string rather than the forward orientation
   * @return a pair of (encoded nucleotide string, minimizer IDs)
   */
  def allMatches(data: NTBitArray, reverseComplement: Boolean = false): (NTBitArray, MinimizerPositions) = {
    if (reverseComplement) {
      val max = data.size - 1
      allMatches(i => BitRepresentation.complementOne(data.apply(max - i)), data.size)
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
   * The data() function is Int->Int rather than Int->Byte, since scala.Function1 is not specialized on Byte
   * return values, causing boxing
   *
   * @param data Function to get the two-bit encoded nucleotide at the given position [0, size)
   * @param size Length of input
   * @return a pair of (encoded nucleotide string, minimizer IDs)
   */
  def allMatches(data: Int => Int, size: Int): (NTBitArray, MinimizerPositions) = {
    var writeLong = 0
    val longs = if (size % 32 == 0) { size / 32 } else { size / 32 + 1 }

    //Array will be be longer than needed and contain extra 0s at the end when there is whitespace
    //TODO use the new NTBitArray API for this
    val encoded = new Array[Long](longs)
    var thisLong = 0L
    //Amount of valid bps we have consumed
    var validSize = 0

    //K-mer table to store minimizer data for each sequence position in a memory efficient way.
    // The tag field will indicate whether the entry is valid (1) at a given position.
    val matches = KmerTable.builder(width, size, 1)
    //zero data plus one tag
    val invalidMinimizer = Array.fill(KmerTable.longsForK(width) + 1)(0L)

    //Position that we are reading from the input
    var pos = 0
    val window = NTBitArray.blank(width)
    while ((validSize < width - 1) && pos < size) {
      val x = data(pos)
      if (x != WHITESPACE) {
        matches.addLongs(invalidMinimizer) //Sets the valid tag to 0
        window.shiftAddBP(x.toByte)
        thisLong = (thisLong << 2) | x

        validSize += 1
        if (validSize % 32 == 0) {
          encoded(writeLong) = thisLong
          writeLong += 1
          thisLong = 0L
        }
      }
      pos += 1
    }
    while (pos < size) {
      val x = data(pos)
      if (x != WHITESPACE) {
        window.shiftAddBP(x.toByte)
        thisLong = (thisLong << 2) | x
        //window will now correspond to the "encoded form" of a motif (reversible mapping to 32-bit Int)
        //priorityOf will give the rank/ID
        val priority = priorities.priorityOf(window)
        if (priority != null) {
          matches.addLongs(priority.data)
          matches.addLong(1) // "valid" tag
        } else {
          matches.addLongs(invalidMinimizer)
        }

        validSize += 1
        if (validSize % 32 == 0) {
          encoded(writeLong) = thisLong
          writeLong += 1
          thisLong = 0L
        }
      }
      pos += 1
    }

    //left-adjust the bits inside the long array
    if (validSize > 0 && validSize % 32 != 0) {
      val finalShift = 64 - (validSize % 32) * 2
      encoded(writeLong) = thisLong << finalShift
    }

    //Remove non-matches from the end of the matches array
    val finalMatches = matches.result(false)
    (NTBitArray(encoded, validSize), new MinimizerPositions(finalMatches, width))
  }

  /**
   * Find all valid matches in a nucleotide string.
   * Returns valid potential minimizers in order, but their positions will not be identifiable.
   *
   * The data() function is Int->Int rather than Int->Byte, since scala.Function1 is not specialized on Byte
   * return values, causing boxing
   *
   * @param data Function to get the two-bit encoded nucleotide at the given position [0, size)
   * @param len Length of input
   * @return valid minimizer IDs
   */
  def matchesOnly(data: Int => Int, len: Int): Iterator[NTBitArray] = new Iterator[NTBitArray] {
    private var pos = 0 //Position that we are reading from the input
    private val window = NTBitArray.blank(width)
    private var result: NTBitArray = null //The next unreturned result, or null if none
    private var consumed = 0

    private def restart(): Unit = {
      window.clear()
      result = null
      consumed = 0
    }

    //Consume at least 'width' valid characters after starting, or after a gap caused by an invalid character
    private def populate(): Unit = {
      restart()
      //First, consume at least 'width' characters, then find the first valid motif
      while ((consumed < width || priorities.priorityOf(window) == null) && pos < len) {
        val x = data(pos)
        pos += 1
        if (x == INVALID) {
          restart()
        } else if (x != WHITESPACE) {
          consumed += 1
          window.shiftAddBP(x.toByte)
        }
      }
      if (consumed >= width) {
        result = priorities.priorityOf(window)
      }
    }

    private def findNext(): Unit = {
      while (result == null && pos < len) {
        val x = data(pos)
        pos += 1

        if (x == INVALID) {
          populate()
        } else if (x != WHITESPACE) {
          window.shiftAddBP(x.toByte)
          result = priorities.priorityOf(window) //compute the result for the next iteration, if any
        }
      }
    }

    populate()

    def hasNext: Boolean = result != null

    def next: NTBitArray = {
      val r = result
      result = null

      findNext()
      r  //The result for this iteration
    }
  }
}
