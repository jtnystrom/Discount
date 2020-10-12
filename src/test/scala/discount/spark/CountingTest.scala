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

package discount.spark

import discount.hash.{MotifExtractor, MotifSpace, ReadSplitter}
import org.scalatest.{FunSuite, Matchers}

class CountingTest extends FunSuite with Matchers with SparkSessionTestWrapper {
  import spark.implicits._

  test("k-mer counting integration test") {
    val data = Seq("ACTGGGTTG", "ACTGTTTTT").toDS()

    val spl = new MotifExtractor(MotifSpace.ofLength(3, false), 4)

    testSplitter(spl)
  }

  def testSplitter[H](spl: ReadSplitter[H]): Unit = {
    val counting = new SimpleCounting(spark, spl, None, None)

    val data = Seq("ACTGGGTTG", "ACTGTTTTT").toDS()
    val verify = List[(String, Long)](
      ("ACTG", 2), ("CTGG", 1), ("TGGG", 1),
      ("CTGT", 1),
      ("GGGT", 1), ("GGTT", 1), ("GTTG", 1),
      ("TGTT", 1), ("GTTT", 1), ("TTTT", 2))

    var counted = counting.countKmers(data).collect()
    counted should contain theSameElementsAs(verify)

    val min2 = new SimpleCounting(spark, spl, Some(2), None)
    counted = min2.countKmers(data).collect()
    counted should contain theSameElementsAs(verify.filter(_._2 >= 2))

    val max1 = new SimpleCounting(spark, spl, None, Some(1))
    counted = max1.countKmers(data).collect()
    counted should contain theSameElementsAs(verify.filter(_._2 <= 1))
  }
}
