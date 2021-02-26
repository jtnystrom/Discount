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

/**
 * Looks for raw motifs in reads, counting them in a histogram.
 */
final class MotifCountingScanner(val space: MotifSpace) extends Serializable {
  @transient
  lazy val scanner = new ShiftScanner(space)

  def scanRead(counter: MotifCounter, read: NTSeq) {
    for { m <- scanner.allMatches(read) } {
      if (! (m eq Motif.Empty)) {
        counter += m
      }
    }
  }

  def scanGroup(counter: MotifCounter, rs: TraversableOnce[NTSeq]) {
    for (r <- rs) scanRead(counter, r)
  }
}