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

import scala.collection.mutable.ArrayBuffer
import discount.util.BitRepresentation._
import discount.util.InvalidNucleotideException

import scala.collection.mutable

/**
 * Bit-shift scanner for fixed width motifs.
 * @param space
 */
final class ShiftScanner(val space: MotifSpace) {

  assert(space.width <= 15)

  val width = space.maxMotifLength

  val mask: Int = {
    var r = 0
    var i = 0
    while (i < width) {
      r = (r << 2) | 3
      i += 1
    }
    r
  }

  val featuresByPriority =
    space.byPriority.zipWithIndex.map(p => Features(p._1, p._2, true))

  /**
   * Find all matches in the string.
   * Returns an array with the matches in order, or Motif.Empty for positions
   * where no valid matches were found.
   */
  def allMatches(data: NTSeq): ArrayBuffer[Motif] = {
    try {
      val r = new ArrayBuffer[Motif](data.length)
      var pos = 0
      var window: Int = 0
      while ((pos < width - 1) && pos < data.length) {
        window = ((window << 2) | charToTwobit(data.charAt(pos))) & mask
        pos += 1
      }

      while (pos < data.length) {
        window = ((window << 2) | charToTwobit(data.charAt(pos))) & mask
        val priority = space.priorityLookup(window)
        if (priority != -1) {
          val features = featuresByPriority(priority)
          val motif = Motif(pos - (width - 1), features)
          r += motif
        } else {
          r += Motif.Empty
        }
        pos += 1
      }
      r
    } catch {
      case ine: InvalidNucleotideException =>
        Console.err.println(s"Unable to parse sequence: '$data' because of character '${ine.invalidChar}' ${ine.invalidChar.toInt}")
        throw ine
    }
  }
}
