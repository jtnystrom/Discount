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
import org.scalatest.matchers.should._

class MinSplitterTest extends AnyFunSuite with Matchers {

  test("Read splitting") {
    val m = 2
    val k = 5
    val test = "AATTTACTTTAGTTAC"
    val space = MinTable.ofLength(m)
    val extractor = MinSplitter(space, k)
    extractor.splitEncode(test).toList.map(_.nucleotides.toString) should equal(
      List("AATTT", "ATTTA", "TTTACTTT", "CTTTA", "TTTAGTTA", "GTTAC"))
  }
}