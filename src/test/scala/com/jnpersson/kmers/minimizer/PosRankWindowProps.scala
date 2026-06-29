/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.kmers.minimizer

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PosRankWindowProps extends AnyFunSuite with ScalaCheckPropertyChecks {
import com.jnpersson.kmers.TestGenerators._

  //The internal list in PosRankWindow should have increasing values of rank (i.e. lower priority)
  //going from beginning to end.
  test("Monotonically increasing rank and position in list") {
    forAll(mAndKPairs) { case (m, k) =>
      forAll(dnaStrings(k), minimizerPriorities(m)) { (x, pri) =>
        whenever(k <= x.length) {
          val scanner = ShiftScanner(pri)
          val motifRanks = scanner.allMatches(x)._2
          val window = new PosRankWindow(m, k, motifRanks)

          while (window.hasNext) {
            window.motifRanks.bitArraySeq.slice(window.leftBound, window.rightBound).
              filter(_ ne MinSplitter.INVALID) shouldBe sorted
            window.advanceWindow()
          }
        }
      }
    }
  }
}
