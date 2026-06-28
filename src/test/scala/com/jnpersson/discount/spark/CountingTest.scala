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

package com.jnpersson.discount.spark

import com.jnpersson.discount.hash.{MinSplitter, MinTable}
import com.jnpersson.discount._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

class CountingTest extends AnyFunSuite with Matchers with SparkSessionTestWrapper {
  import spark.implicits._
  implicit val s: SparkSession = spark

  def makeCounting(reads: Dataset[String], spl: AnyMinSplitter,
                   min: Option[Int], max: Option[Int],
                   orientation: Orientation): CountedKmers = {
    val bspl = spark.sparkContext.broadcast(spl)
    GroupedSegments.fromReads(reads, Simple, orientation == ForwardOnly, bspl).
      toIndex(orientation).filterCounts(min, max).counted(orientation)
  }

  test("k-mer counting integration test") {
    val spl = new MinSplitter(MinTable.ofLength(3), 4)
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

    var counted = makeCounting(data, spl, None, None, Both).withSequences.collect()
    counted should contain theSameElementsAs verify

    counted = makeCounting(data, spl, None, None, ForwardOnly).withSequences.collect()
    counted should contain theSameElementsAs onlyForwardVerify

    counted = makeCounting(data, spl, Some(2), None, Both).withSequences.collect()
    counted should contain theSameElementsAs verify.filter(_._2 >= 2)

    counted = makeCounting(data, spl, None, Some(1), Both).withSequences.collect()
    counted should contain theSameElementsAs verify.filter(_._2 <= 1)
  }

  def test10kCounting(minSource: MinimizerSource, m: Int, ordering: MinimizerOrdering): Unit = {
    val k = 31
    val discount = new Discount(k, minSource, m, ordering)
    val index = discount.index("testData/SRR094926_10k.fasta")
    val all = index.totalStats()

    all.equalCounts(Testing.correctStats10k31) should be(true)
  }

  test("10k reads, lexicographic") {
    test10kCounting(All, 7, Lexicographic)
  }

  test("10k reads, signature") {
    test10kCounting(All, 7, Signature)
  }

  test("10k reads, random") {
    test10kCounting(All, 7, XORMask())
  }

  test("10k reads, universal lexicographic") {
    test10kCounting(Bundled, 9, Lexicographic)
  }

  test("10k reads, universal frequency") {
    test10kCounting(Bundled, 9, Frequency())
  }

  test("single long sequence") {
    val k = 31
    val m = 10
    val discount = new Discount(k, All, m, ordering = Lexicographic)
    val index = discount.index("testData/Akashinriki_10k.fasta")
    val all = index.totalStats()

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
    val index = discount.index("testData/ERR599052_10k.fastq")
    val all = index.totalStats()

    //Reference values computed with Jellyfish
    all.totalAbundance should equal(691827)
    all.distinctKmers should equal(691078)
    all.uniqueKmers should equal(690499)
    all.maxAbundance should equal(23)
  }
}
