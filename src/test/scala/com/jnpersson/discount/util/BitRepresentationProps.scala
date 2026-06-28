/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers._

class BitRepresentationProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import BitRepresentation._

  test("bytesToString reversible") {
    forAll(dnaStringsMixedCase(1, 200)) { x =>
      val len = x.length
      val builder = new StringBuilder
      bytesToString(stringToBytes(x), builder, 0, len) should equal(x.toUpperCase())
    }
  }

  test("DNAHelpers reverseComplement") {
    forAll(dnaStrings) { x =>
      DNAHelpers.reverseComplement(DNAHelpers.reverseComplement(x)) should equal(x)
    }
  }

  test("Bitwise reverseComplement") {
    forAll(ms) { m =>
      whenever (m >= 1 && m <= 32) {
        val mask = -1L >>> (64 - 2 * m)

        forAll(encodedMinimizers(m)) { min =>
          val rev = BitRepresentation.reverseComplement(min, m, mask)
          val revrev = BitRepresentation.reverseComplement(rev, m, mask)
          revrev should equal(min)
        }
      }
    }
  }

  test("Reverse complement populates NTBitArray") {
    forAll(ms) { m =>
      whenever (m >= 1 && m <= 32) {
        val mask = -1L >>> (64 - 2 * m)

        forAll(encodedMinimizers(m)) { min =>
          val rev = BitRepresentation.reverseComplement(min, m, mask)

          val ntb1 = NTBitArray.fromLong(min, m)
          val ntb2 = NTBitArray.fromLong(rev, m)
          ntb1.reverseComplement should equal(ntb2)
        }
      }
    }
  }

  test("Left-aligned reverse complement") {
    forAll(ms) { m =>
      whenever(m >= 1 && m <= 32) {
        forAll(encodedMinimizers(m)) { min =>
          val lmin = min << (64 - 2 * min) //left aligned
          val lrev = BitRepresentation.reverseComplementLeftAligned(lmin, -1)
          val lrevrev = BitRepresentation.reverseComplementLeftAligned(lrev, -1)
          lrevrev should equal(lmin)
        }
      }
    }
  }

}
