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

package discount.hash

import org.scalacheck.{Prop, Properties}
import Prop._

class PosRankWindowProps extends Properties("PosRankWindow") {
import discount.TestGenerators._

  property("Window top item is at or after window start") = forAll(dnaReads, ms, ks) { (x, m, k) =>
    ( m >= 1 && k >= 1 && k <= x.length && m <= k) ==> {
      val cache = new FastTopRankCache

      val space = MotifSpace.ofLength(m, false)
      val motifs = x.sliding(m).zipWithIndex.map(x => space.get(x._1, x._2)).toList

      val topItems = motifs.map(mot => {
        cache.moveWindowAndInsert(mot.pos, mot)
        (cache.top, mot.pos)
      })
      //No window can have a top item with a position prior to that window's start position
      topItems.filter { case (mot, winStart) => (mot.pos < winStart) }.isEmpty
    }
  }
}
