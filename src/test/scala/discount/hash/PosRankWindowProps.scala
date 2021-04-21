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
    forAll(dnaStrings, ms, ks) { (x, m, k) =>
      whenever ( m >= 1 && k >= m && k <= x.length) {
        val window = new PosRankWindow

        val space = Testing.motifSpace(m)
        val motifs = x.sliding(m).zipWithIndex.map(x => space.get(x._1, x._2)).toList

        val topItems = motifs.map(mot => {
          val winStart = mot.pos + (m - k)
          window.moveWindowAndInsert(winStart, mot)
          (window.top, winStart)
        })

        for ((mot, winStart) <- topItems) {
          mot.pos should (be >= winStart)
          mot.pos should (be <= (winStart + (k - 1)))
        }
      }
    }
  }

  //The internal list in PosRankWindow should have increasing values of rank (i.e. lower priority)
  //going from beginning to end.
  test("Monotonically increasing rank in list") {
    forAll(dnaStrings, ms, ks) { (x, m, k) =>
      whenever(m >= 1 && k > m && k <= x.length) {
        val window = new PosRankWindow
        val space = Testing.motifSpace(m)
        val motifs = x.sliding(m).zipWithIndex.map(x => space.get(x._1, x._2)).toList

        for (mot <- motifs) {
          window.moveWindowAndInsert(mot.pos + (m - k), mot)
          if (window.size >= 2) {
            val oooItems = window.toSeq.sliding(2).filter(x => (x(0).rank > x(1).rank)).toList
            oooItems should be (empty)
          }
        }
      }
    }
  }
}
