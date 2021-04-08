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

import discount.Testing
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PosRankWindowProps extends AnyFunSuite with ScalaCheckPropertyChecks {
import discount.TestGenerators._

  test("Window top item is inside k-length window") {
    forAll(dnaReads, ms, ks) { (x, m, k) =>
      whenever ( m >= 1 && k >= m && k <= x.length) {
        val cache = new FastTopRankCache

        val space = Testing.motifSpace(m)
        val motifs = x.sliding(m).zipWithIndex.map(x => space.get(x._1, x._2)).toList

        val topItems = motifs.map(mot => {
          val winStart = mot.pos + (m - k)
          cache.moveWindowAndInsert(winStart, mot)
          (cache.top, winStart)
        })

        for ((mot, winStart) <- topItems) {
          mot.pos should (be >= winStart)
          mot.pos should (be <= (winStart + (k - 1)))
        }
      }
    }
  }

  //TODO repair this property by making PosRankWindow an Iterable
//  //The internal list in PosRankWindow should have increasing values of rank (i.e. lower priority)
//  //going from beginning to end.
//  test("Monotonically increasing rank in list") {
//    forAll(dnaReads, ms, ks) { (x, m, k) =>
//      whenever(m >= 1 && k > m && k <= x.length) {
//        val cache = new FastTopRankCache
//        val space = Testing.motifSpace(m)
//        val motifs = x.sliding(m).zipWithIndex.map(x => space.get(x._1, x._2)).toList
//
//        for (mot <- motifs) {
//          cache.moveWindowAndInsert(mot.pos + (m - k), mot)
//          if (cache.cache.size >= 2) {
//            val oooItems = cache.cache.toSeq.sliding(2).filter(x => (x(0).rank > x(1).rank)).toList
//            oooItems should be (empty)
//          }
//        }
//      }
//    }
//  }
}
