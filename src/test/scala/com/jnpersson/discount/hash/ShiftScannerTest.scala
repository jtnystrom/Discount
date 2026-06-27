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

package com.jnpersson.discount.hash

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

class ShiftScannerTest extends AnyFunSuite with Matchers {

  test("motif counting") {
    val space = MinTable.ofLength(3)
    val reads = Seq("ACTGTT", "TGGTTCCA")
    val scanner = ShiftScanner(space)
    val matches = reads.flatMap(r => scanner.allMatches(r)._2.validBitArrayIterator).
      map(m => space.motifArray(m.toInt).toString)

    matches should contain theSameElementsAs
      List("ACT", "CTG", "TGT", "GTT", "TGG", "GGT", "GTT", "TTC", "TCC", "CCA")
  }

}
