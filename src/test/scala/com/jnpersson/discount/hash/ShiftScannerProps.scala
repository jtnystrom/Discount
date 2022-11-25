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
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ShiftScannerProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import com.jnpersson.discount.TestGenerators._

  test("Find all m-mers") {
    forAll(Gen.choose(1, 10)) { m =>
      forAll(dnaStrings(m, 100)) { x =>
        val space = Testing.minTable(m)
        val scanner = space.scanner
        scanner.allMatches(x)._2.drop(m - 1).map(
          x => space.byPriority(x.toInt)).toList should equal(x.sliding(m).toList)
      }
    }
  }

  test("Encoding of NT sequence") {
    forAll(Gen.choose(1, 10)) { m =>
      forAll(dnaStrings(m, 100)) { x =>
        val space = Testing.minTable(m)
        val scanner = space.scanner
        scanner.allMatches(x)._1.toString should equal(x)
      }
    }
  }
}
