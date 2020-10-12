/*
 * This file is part of Discount. Copyright (c) 2020 Johan Nyström-Persson.
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
import org.scalatest.{FunSuite, Matchers}

//Most tests in this file are sensitive to the order of markers in the all2 space
class PosRankWindowTest extends FunSuite with Matchers {
  import Testing._

  test("position") {
    //TODO repair this test and rewrite for FastTopRankCache

//    val test = ms(Seq(("AC", 3), ("AC", 5), ("GT", 10)))
//    val prl = PosRankWindow(test)
//    prl should contain theSameElementsAs(test)
//
//    prl.dropUntilPosition(2)
//    prl should contain theSameElementsAs(test)
//
//    prl.dropUntilPosition(3)
//    prl should contain theSameElementsAs(test)
//    prl.dropUntilPosition(4)
//    prl should contain theSameElementsAs(test.drop(1))
//    prl.dropUntilPosition(6)
//    prl should contain theSameElementsAs(test.drop(2))
//    prl.dropUntilPosition(11)
//    prl should contain theSameElementsAs(Seq())
//
//    assert(prl.end === End())
  }

}