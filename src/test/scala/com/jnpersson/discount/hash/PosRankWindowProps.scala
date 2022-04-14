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
import com.jnpersson.discount.Testing
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PosRankWindowProps extends AnyFunSuite with ScalaCheckPropertyChecks {
import com.jnpersson.discount.TestGenerators._

  //The internal list in PosRankWindow should have increasing values of rank (i.e. lower priority)
  //going from beginning to end.
  test("Monotonically increasing rank and position in list") {
    forAll(dnaStrings, ms, ks) { (x, m, k) =>
      whenever(m >= 1 && k > m && k <= x.length) {
        val space = Testing.motifSpace(m)
        val scanner = new ShiftScanner(space)
        val motifRanks = scanner.allMatches(x)._2
        val window = new PosRankWindow(m, k, motifRanks)

        while (window.hasNext) {
          window.motifRanks.slice(window.leftBound, window.rightBound).filter(_ != MinSplitter.INVALID) shouldBe sorted
          window.advanceWindow()
        }
      }
    }
  }

}
