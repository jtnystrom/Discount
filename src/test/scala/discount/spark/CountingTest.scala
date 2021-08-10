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

  test("k-mer counting integration test") {
    val spl = new MotifExtractor(MotifSpace.ofLength(3, false), 4)
    testSplitter(spl)
  }

  def makeCounting(reads: Dataset[String], spl: MotifExtractor,
                      min: Option[Abundance], max: Option[Abundance],
                      normalize: Boolean) = {
    val bspl = spark.sparkContext.broadcast(spl)
    GroupedSegments.fromReads(reads, bspl).counting(min, max, normalize)
  }

  def testSplitter(spl: MotifExtractor): Unit = {

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

  def test10kCounting(space: MotifSpace): Unit = {
    val k = 31
    val spl = new MotifExtractor(space, k)
    val bcSpl = spark.sparkContext.broadcast(spl)
    val ir = new InputReader(1000, k, false)
    val reads = ir.getReadsFromFiles("testData/SRR094926_10k.fasta", false)
    val grouped = GroupedSegments.fromReads(reads, bcSpl)
    val counting = grouped.counting(None, None, false)
    val stats = counting.bucketStats
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
    val k = 31
    val sampling = new Sampling
    val space = MotifSpace.ofLength(m, false)
    val motifs = spark.read.csv("PASHA/pasha_all_28_9.txt").collect().map(_.getString(0))
    val limitedSpace = MotifSpace.fromTemplateWithValidSet(space, motifs)
    val ir = new InputReader(1000, k, false)
    val sampledReads = ir.getReadsFromFiles("testData/SRR094926_10k.fasta",
      false, Some(0.01))
    val sampledSpace = sampling.createSampledSpace(sampledReads, limitedSpace, 1, None)
    test10kCounting(sampledSpace)
  }
}
