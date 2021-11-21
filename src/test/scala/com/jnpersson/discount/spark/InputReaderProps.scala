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

package com.jnpersson.discount.spark

import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers._

class InputReaderProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import com.jnpersson.discount.TestGenerators._

  test("Correct splitting of multiline input into fragments") {
    forAll(fastaSequences(50, 1, 20), ks) { (x, k) =>
      whenever(k < x.length && k >= 10 && k < 50) {
        val parser = FragmentParser(k, None, 100, true)
        val f = BufferFragment("x", 1, x.getBytes, 0, x.length - 1)
        val pure = x.replaceAll("\n", "")
        val spl = parser.splitFragment(f).toList

        (spl.map(s => s.nucleotides.dropRight(k - 1)).mkString("") + spl.last.nucleotides.takeRight(k - 1)) should
          equal(pure)

        spl.flatMap(s => s.location.until(s.location + s.nucleotides.length - (k - 1))) should
          equal((1 until (1 + pure.length - (k - 1))))
      }
    }
  }

}
