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

import com.jnpersson.kmers.TestGenerators.{abundances, encodedSupermers}
import com.jnpersson.kmers._
import com.jnpersson.kmers.minimizer._
import com.jnpersson.discount.bucket.{BucketStats, ReduceParams, Reducer, ReducibleBucket, Tag}
import com.jnpersson.discount.bucket.Rule.Sum
import com.jnpersson.kmers.util.NTBitArray
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalacheck.Gen
import org.scalacheck.util.Buildable
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

object CountingTest {
  def correctStats10k31: BucketStats = {
    //Reference values computed with Jellyfish
    BucketStats("", 0, 698995, 692378, 686069, 8)
  }

  /** Helper methods for testing of ReducibleBucket */
  implicit class TestEnhancedBucket(b: ReducibleBucket) {
    def totalCount = b.tags.flatten.sum
    def distinctKmers = b.tags.map(_.length).sum
    def stats = BucketStats.collectFromCounts("", b.tags)
  }
}

object CountingTestGenerators {
  def kmerTags(n: Int): Gen[Array[Tag]] =
    Gen.listOfN(n, abundances).map(_.toArray)

  def kmerTags(sm: NTBitArray, k: Int): Gen[Array[Tag]] = kmerTags(sm.size - (k - 1))

  def kmerTags(sms: Array[NTBitArray], k: Int): Gen[Seq[Array[Tag]]] =
    Gen.sequence(sms.map(sm => kmerTags(sm, k)))(Buildable.buildableSeq)

  def reducibleBucket(k: Int): Gen[ReducibleBucket] = {
    val sumReducer = Reducer.configure(
      ReduceParams(k), Sum)
    for {
      nSupermers <- Gen.choose(1, 10)
      supermers <- Gen.listOfN(nSupermers, encodedSupermers(k)).map(_.toArray)
      tags <- kmerTags(supermers, k)
      b = ReducibleBucket(0, supermers, tags.toArray)
    } yield b.reduceCompact(sumReducer)
  }

  //Generate a pair of buckets that have distinct super-mers and also common super-mers.
  //For the common super-mers, the tags (counts) need not be the same for the two buckets.
  def bucketPairWithCommonKmers(k: Int): Gen[(ReducibleBucket, ReducibleBucket)] = {
    val sumReducer = Reducer.configure(ReduceParams(k), Sum)
    for {
      bucket1 <- reducibleBucket(k)
      bucket2 <- reducibleBucket(k)
      n <- Gen.choose(1, 10)
      commonSupermers <- Gen.listOfN(n, encodedSupermers(k)).map(_.toArray)
      tags1 <- kmerTags(commonSupermers, k)
      tags2 <- kmerTags(commonSupermers, k)
      bc1 = bucket1.appendAndCompact(ReducibleBucket(0, commonSupermers, tags1.toArray), sumReducer)
      bc2 = bucket2.appendAndCompact(ReducibleBucket(0, commonSupermers, tags2.toArray), sumReducer)
    } yield (bc1, bc2)
  }
}

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

    all.equalCounts(CountingTest.correctStats10k31) should be(true)
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
