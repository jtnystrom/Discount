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
import discount.hash.{MotifExtractor, MotifSpace, Orderings, ReadSplitter}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

class CountingTest extends AnyFunSuite with Matchers with SparkSessionTestWrapper {
  import spark.implicits._
  implicit val s = spark

  test("k-mer counting integration test") {
    val spl = new MotifExtractor(MotifSpace.ofLength(3, false), 4)
    testSplitter(spl)
  }

  def testSplitter[H](spl: ReadSplitter[H]): Unit = {
    val counting = new SimpleCounting(spl, None, None, false)
    val nCounting = new SimpleCounting(spl, None, None, true)

    val data = Seq("AACTGGGTTG", "ACTGTTTTT").toDS()
    val verify = List[(String, Long)](
      ("AACT", 1),
      ("ACTG", 2), ("CTGG", 1), ("TGGG", 1),
      ("CTGT", 1),
      ("GGGT", 1), ("GGTT", 1), ("GTTG", 1),
      ("TGTT", 1), ("GTTT", 1), ("TTTT", 2))

    val onlyForwardVerify = List[(String, Long)](
      ("AACT", 1),
      ("ACTG", 2)
    )

    var counted = counting.countKmers(data).collect()
    counted should contain theSameElementsAs(verify)

    counted = nCounting.countKmers(data).collect()
    counted should contain theSameElementsAs(onlyForwardVerify)

    val min2 = new SimpleCounting(spl, Some(2), None, false)
    counted = min2.countKmers(data).collect()
    counted should contain theSameElementsAs(verify.filter(_._2 >= 2))

    val max1 = new SimpleCounting(spl, None, Some(1), false)
    counted = max1.countKmers(data).collect()
    counted should contain theSameElementsAs(verify.filter(_._2 <= 1))
  }

  def test10kCounting(space: MotifSpace): Unit = {
    val k = 31
    val spl = new MotifExtractor(space, k)
    val counting = new SimpleCounting(spl, None, None, false)
    val reads = counting.routines.getReadsFromFiles("testData/SRR094926_10k.fasta",
      false, 1000, k)
    val stats = counting.getStatistics(reads, false)
    val all = stats.collect().reduce(_ merge _)
    all.totalAbundance should equal(698995)
    all.distinctKmers should equal(692378)
    all.uniqueKmers should equal(686069)
    all.maxAbundance should equal(8)
  }

  test("10k reads, lexicographic") {
    val m = 7
    val space = MotifSpace.ofLength(m, false)
    test10kCounting(space)
  }

  test("10k reads, signature") {
    val m = 7
    val space = MotifSpace.ofLength(m, false)
    val sig = Orderings.minimizerSignatureSpace(space)
    test10kCounting(sig)
  }

  test("10k reads, random") {
    val m = 7
    val space = MotifSpace.ofLength(m, false)
    val rnd = Orderings.randomOrdering(space)
    test10kCounting(rnd)
  }

  test("10k reads, universal lexicographic") {
    val m = 9
    val space = MotifSpace.ofLength(m, false)
    val motifs = spark.read.csv("PASHA/pasha_all_28_9.txt").collect().map(_.getString(0))
    val limitedSpace = MotifSpace.fromTemplateWithValidSet(space, motifs)
    test10kCounting(limitedSpace)
  }

  test("10k reads, universal frequency") {
    val m = 9
    val routines = new Routines(spark)
    val space = MotifSpace.ofLength(m, false)
    val motifs = spark.read.csv("PASHA/pasha_all_28_9.txt").collect().map(_.getString(0))
    val limitedSpace = MotifSpace.fromTemplateWithValidSet(space, motifs)
    val reads = routines.getReadsFromFiles("testData/SRR094926_10k.fasta",
      false, 1000, 31, Some(0.01))
    val sampledSpace = routines.createSampledSpace(reads, limitedSpace, 1, None)
    test10kCounting(sampledSpace)
  }
}
