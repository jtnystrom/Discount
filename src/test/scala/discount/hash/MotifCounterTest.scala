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

import org.scalatest.{FunSuite, Matchers}

class MotifCounterTest extends FunSuite with Matchers {

  test("basic") {
    val space = MotifSpace.ofLength(3, false)
    val reads = Seq("ACTGTT", "TGGTTCCA")
    val counter = MotifCounter(space)
    val scanner = new MotifCountingScanner(space)

    for (r <- reads) {
      scanner.scanRead(counter, r)
    }
    counter.motifsWithCounts(space).toSeq.filter(_._2 > 0) should contain theSameElementsAs(
      List[(String, Int)](
        ("ACT", 1), ("CTG", 1), ("TGT", 1),
        ("GTT", 2), ("TGG", 1), ("GGT", 1),
        ("TTC", 1), ("TCC", 1), ("CCA", 1)
      ))

  }

}
