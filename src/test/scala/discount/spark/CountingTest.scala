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

import discount._
import discount.hash.{MotifExtractor, MotifSpace, Orderings, ReadSplitter}
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

class CountingTest extends AnyFunSuite with Matchers with SparkSessionTestWrapper {
  import spark.implicits._
  implicit val s = spark

  def makeCounting(reads: Dataset[String], spl: MotifExtractor,
                      min: Option[Abundance], max: Option[Abundance],
                      normalize: Boolean) = {
    val bspl = spark.sparkContext.broadcast(spl)
    GroupedSegments.fromReads(reads, bspl).counting(min, max, normalize)
  }

  test("k-mer counting integration test") {
    val spl = new MotifExtractor(MotifSpace.ofLength(3, false), 4)
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

    var counted = makeCounting(data, spl, None, None, false).counts.withSequences.collect()
    counted should contain theSameElementsAs(verify)

    counted = makeCounting(data, spl, None, None, true).counts.withSequences.collect()
    counted should contain theSameElementsAs(onlyForwardVerify)

    counted = makeCounting(data, spl, Some(2), None, false).counts.withSequences.collect()
    counted should contain theSameElementsAs(verify.filter(_._2 >= 2))

    counted = makeCounting(data, spl, None, Some(1), false).counts.withSequences.collect()
    counted should contain theSameElementsAs(verify.filter(_._2 <= 1))
  }

  def test10kCounting(minimizerFile: Option[String], m: Int, ordering: String): Unit = {
    val k = 31
    val discount = new Discount(k, minimizerFile, m, ordering, samplePartitions = 1)
    val kmers = discount.kmers("testData/SRR094926_10k.fasta")
    val stats = kmers.counting().bucketStats
    val all = stats.collect().reduce(_ merge _)
    all.totalAbundance should equal(698995)
    all.distinctKmers should equal(692378)
    all.uniqueKmers should equal(686069)
    all.maxAbundance should equal(8)
  }

  test("10k reads, lexicographic") {
    test10kCounting(None, 7, "lexicographic")
  }

  test("10k reads, signature") {
    test10kCounting(None, 7, "signature")
  }

  test("10k reads, random") {
    test10kCounting(None, 7, "random")
  }

  test("10k reads, universal lexicographic") {
    test10kCounting(Some("PASHA/pasha_all_28_9.txt"), 9, "lexicographic")
  }

  test("10k reads, universal frequency") {
    test10kCounting(Some("PASHA/pasha_all_28_9.txt"), 9, "frequency")
  }
}
