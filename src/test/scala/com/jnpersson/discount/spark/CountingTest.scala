/*
 * This file is part of Discount. Copyright (c) 2022 Johan Nyström-Persson.
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

import com.jnpersson.discount.Abundance
import com.jnpersson.discount.hash.{MinSplitter, MotifSpace}
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

class CountingTest extends AnyFunSuite with Matchers with SparkSessionTestWrapper {
  import spark.implicits._
  implicit val s = spark

  def makeCounting(reads: Dataset[String], spl: AnyMinSplitter,
                   min: Option[Abundance], max: Option[Abundance],
                   normalize: Boolean): CountedKmers = {
    val bspl = spark.sparkContext.broadcast(spl)
    GroupedSegments.fromReads(reads, Simple(normalize), bspl).
      counting(min, max, normalize).counts
  }

  test("k-mer counting integration test") {
    val spl = new MinSplitter(MotifSpace.ofLength(3), 4)
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

    var counted = makeCounting(data, spl, None, None, normalize = false).withSequences.collect()
    counted should contain theSameElementsAs verify

    counted = makeCounting(data, spl, None, None, normalize = true).withSequences.collect()
    counted should contain theSameElementsAs onlyForwardVerify

    counted = makeCounting(data, spl, Some(2), None, normalize = false).withSequences.collect()
    counted should contain theSameElementsAs verify.filter(_._2 >= 2)

    counted = makeCounting(data, spl, None, Some(1), normalize = false).withSequences.collect()
    counted should contain theSameElementsAs verify.filter(_._2 <= 1)
  }

  def test10kCounting(minSource: MinimizerSource, m: Int, ordering: String): Unit = {
    val k = 31
    val discount = new Discount(k, minSource, m, ordering)
    val kmers = discount.kmers("testData/SRR094926_10k.fasta")
    val stats = kmers.segments.counting().bucketStats
    val all = stats.collect().reduce(_ merge _)

    //Reference values computed with Jellyfish
    all.totalAbundance should equal(698995)
    all.distinctKmers should equal(692378)
    all.uniqueKmers should equal(686069)
    all.maxAbundance should equal(8)
  }

  test("10k reads, lexicographic") {
    test10kCounting(All, 7, "lexicographic")
  }

  test("10k reads, signature") {
    test10kCounting(All, 7, "signature")
  }

  test("10k reads, random") {
    test10kCounting(All, 7, "random")
  }

  test("10k reads, universal lexicographic") {
    test10kCounting(Path("resources/PASHA/minimizers_28_9.txt"), 9, "lexicographic")
  }

  test("10k reads, universal frequency") {
    test10kCounting(Path("resources/PASHA/minimizers_28_9.txt"), 9, "frequency")
  }

  test("single long sequence") {
    val k = 31
    val m = 10
    val discount = new Discount(k, All, m, ordering = "lexicographic")
    val kmers = discount.kmers("testData/Akashinriki_10k.fasta")
    val stats = kmers.segments.counting().bucketStats
    val all = stats.collect().reduce(_ merge _)

    //Reference values computed with Jellyfish
    all.totalAbundance should equal(485168)
    all.distinctKmers should equal(419554)
    all.uniqueKmers should equal(377145)
    all.maxAbundance should equal(12)
  }

  test("fastq format") {
    val k = 31
    val m = 10
    val discount = new Discount(k, All, m)
    val kmers = discount.kmers("testData/ERR599052_10k.fastq")
    val stats = kmers.segments.counting().bucketStats
    val all = stats.collect().reduce(_ merge _)

    //Reference values computed with Jellyfish
    all.totalAbundance should equal(691827)
    all.distinctKmers should equal(691078)
    all.uniqueKmers should equal(690499)
    all.maxAbundance should equal(23)
  }
}
