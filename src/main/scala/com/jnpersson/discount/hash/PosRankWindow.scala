/*
 * This file is part of Discount. Copyright (c) 2021 Johan Nyström-Persson.
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

/**
 * Tracks Motifs in a moving window, such that the top priority item can always be obtained efficiently.
 * Mutates the array. Can only be used once.
 * This object looks like an Iterator[Int], but to avoid boxing of integers, does not extend that trait.
 *
 * Invariants: the leftmost position has the highest priority (minimal rank).
 * Priority decreases (i.e. rank increases) monotonically going left to right.
 * Motifs are sorted by position.
 * The minimizer of the current k-length window is always the first motif in the list.
 * @param motifRanks Array of motif priorities at the positions in the underlying read where the full motif can first
 *                   be read (e.g. position 4 for a 5-length motif occupying positions 0-4).
 */
final class PosRankWindow(m: Int, k: Int, val motifRanks: Array[Int]) {
  import Motif.INVALID

  //>= start of k -(m-1)-length window. The current minimizer will be at this position in the array.
  //Represents a k-length window in the underlying sequence (the first m-mer can only be read at position (m-1))
  var leftBound = 0

  //End of m-length window, not inclusive (1 past the end)
  var rightBound = 1

  //Initialize
  while (rightBound < k) {
    advanceWindow()
  }

  def advanceWindow(): Unit = {
    rightBound += 1
    if (rightBound > motifRanks.length) {
      return
    }
    //new motif in window
    val inserted = motifRanks(rightBound - 1)
    if (inserted != INVALID) {
      var test = rightBound - 2
      //Ensure monotonic by blanking out (setting to INVALID) motifs that
      //can never be minimizers
      while (test >= leftBound + 1 &&
        (motifRanks(test) == INVALID || motifRanks(test) > inserted)) {
        motifRanks(test) = INVALID
        test -= 1
      }
      //newly inserted motif is the new minimizer; force leftBound to advance
      if (inserted < motifRanks(leftBound)) {
        leftBound += 1
      }
    }
    //Advance leftBound to a valid item if the current item is invalid.
    //Also advance if the window is too wide.
    while (rightBound - leftBound > k - (m - 1) ||
      (leftBound < motifRanks.length && motifRanks(leftBound) == INVALID)) {
      leftBound += 1
    }
  }

  def head: Int = leftBound

  def next: Int = {
    val pos = leftBound
    advanceWindow()
    pos
  }

  def hasNext = rightBound <= motifRanks.length
}