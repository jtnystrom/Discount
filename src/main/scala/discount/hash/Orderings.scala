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

object Orderings {

  /**
   * Create a MotifSpace that de-prioritizes motifs where the motif
   * 1. Starts with AAA or ACA, or
   * 2. Contains AA anywhere except the beginning
   *
   * This is not the most efficient approach in practice, but useful as a baseline to benchmark against.
   * @return
   */

  def minimizerSignatureSpace(w: Int): MotifSpace = {
    val template = MotifSpace.ofLength(w, false)
    val all = template.byPriority
    val withCounts = all.map(mot => (mot, priority(mot)))
    MotifCounter.toSpaceByFrequency(withCounts, template.byPriority)
  }

  /**
   * Generate a pseudo-count for each motif.
   * Lower numbers have higher priority.
   * Here we use count "1" to indicate a low priority motif.
   */
  def priority(motif: String): Int = {
    val i = motif.indexOf("AA")
    if (i != -1 && i > 0) {
      return 1
    }

    if (motif.startsWith("AAA") || motif.startsWith("ACA")) {
      return 1
    }
    0
  }
}
