/*
 * This file is part of Discount. Copyright (c) 2020 Johan Nyström-Persson.
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

package discount.hash

import discount.NTSeq
import discount.hash.Motif.Features
import discount.util.BitRepresentation._
import discount.util.InvalidNucleotideException

/**
 * Bit-shift scanner for fixed width motifs. Identifies all valid (according to some [[MotifSpace]])
 * motifs in a sequence.
 * @param space The space to scan for motifs of
 */
final class ShiftScanner(val space: MotifSpace) {

  assert(space.width <= 15)

  val width: Int = space.width

  //Int bitmask with the rightmost 2 * width bits set to 1
  val mask: Int = {
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
   * Find all matches in the string.
   * Returns the matches in order, or Motif.Empty for positions
   * where no valid matches were found.
   */
  def allMatches(data: NTSeq): Iterator[Motif] = {
    try {
      var pos = 0
      var window: Int = 0
      while ((pos < width - 1) && pos < data.length) {
        window = ((window << 2) | charToTwobit(data.charAt(pos)))
        pos += 1
      }

      new Iterator[Motif] {
        def hasNext = pos < data.length

        def next: Motif = {
          window = ((window << 2) | charToTwobit(data.charAt(pos))) & mask
          //window will now correspond to the "encoded form" of a motif (reversible mapping to 32-bit Int)
          //priorityLookup will give the rank/ID
          val priority = space.priorityLookup(window)
          pos += 1

          if (priority != -1) {
            //pos - (width - 1) - 1
            val motifPos = pos - width
            val features = featuresByPriority(priority)
            Motif(motifPos, features)
          } else {
            Motif.Empty
          }
        }
      }
    } catch {
      case ine: InvalidNucleotideException =>
        Console.err.println(s"Unable to parse sequence: '$data' because of character '${ine.invalidChar}' ${ine.invalidChar.toInt}")
        if (ine.invalidChar == '\n') {
          Console.err.println("Do you need to enable support for multiline FASTA files? (--multiline)")
        }
        throw ine
    }
  }

  /**
   * Count motifs in a read and add them to the supplied MotifCounter.
   * @param counter
   * @param read
   */
  def countMotifs(counter: MotifCounter, read: NTSeq) {
    for { m <- allMatches(read) } {
      if (! (m eq Motif.Empty)) {
        counter increment m
      }
    }
  }

  def countMotifs(counter: MotifCounter, reads: TraversableOnce[NTSeq]) {
    for (r <- reads) countMotifs(counter, r)
  }
}
