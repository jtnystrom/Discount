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

package com.jnpersson.discount.util

import com.jnpersson.discount.TestGenerators._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BitRepresentationProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import BitRepresentation._
  import DNAHelpers._

  test("bytesToString reversible") {
    forAll(dnaStrings) { x =>
      whenever (x.length >= 1) {
        val len = x.length
        val builder = new StringBuilder
        bytesToString(stringToBytes(x), builder, 0, len) should equal(x)
      }
    }
  }

  test("DNAHelpers reverseComplement") {
    forAll(dnaStrings) { x =>
      whenever (x.length >= 1) {
        reverseComplement(reverseComplement(x)) should equal(x)
      }
    }
  }
}
