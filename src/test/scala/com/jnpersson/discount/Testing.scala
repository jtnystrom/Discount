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

package com.jnpersson.discount

import com.jnpersson.discount.spark.Rule.Sum
import com.jnpersson.discount.bucket.{BucketStats, ReduceParams, Reducer, ReducibleBucket, Tag}

import scala.collection.{immutable, mutable}
import com.jnpersson.discount.hash.{MinSplitter, MinTable, MinimizerPriorities, RandomXOR, SpacedSeed}
import com.jnpersson.discount.spark.{Index, IndexParams}
import com.jnpersson.discount.util.{BitRepresentation, NTBitArray}
import org.apache.spark.sql.SparkSession
import org.scalacheck.Gen.Parameters
import org.scalacheck.Shrink.{shrink, shrinkContainer}
import org.scalacheck.rng.Seed
import org.scalacheck.util.Buildable
import org.scalacheck.{Gen, Shrink}

object Testing {
  //Cache these so that we can test many properties efficiently
  //without allocating this big object each time
  private val spaces = mutable.Map[Int, MinTable]()
  def minTable(m: Int): MinTable = synchronized {
    spaces.get(m) match {
      case Some(s) => s
      case _ =>
        val space = MinTable.ofLength(m)
        spaces(m) = space
        space
    }
  }

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

  /** Generate a list of items when we don't care about preserving parameters.
   * This only works if the generator is guaranteed to succeed. */
  def getList[T](gen: Gen[T], n: Int): immutable.Seq[T] =
    getSingle(Gen.listOfN(n, gen))

  /** Generate an item when we don't care about preserving parameters.
   * This only works if the generator is guaranteed to succeed. */
  def getSingle[T](gen: Gen[T]): T =
    gen.apply(Parameters.default, Seed(System.currentTimeMillis())).get
}

object TestGenerators {
  import BitRepresentation._

  val dnaCharsArray = "ACTG".toArray
  val dnaCharsArrayMixedCase = "ACTGactg".toArray
  val dnaRnaCharsArrayMixedCase = "ACTGUactgu".toArray
  val dnaLetterTwobits: Gen[Byte] = Gen.choose(0, 3).map(x => twobits(x))

  def dnaStrings(minLen: Int, maxLen: Int): Gen[NTSeq] = for {
    length <- Gen.choose(minLen, maxLen)
    x <- Gen.stringOfN(length, Gen.oneOf(dnaCharsArray))
  } yield x

  def dnaStringsMixedCase(minLen: Int, maxLen: Int): Gen[NTSeq] = for {
    length <- Gen.choose(minLen, maxLen)
    x <- Gen.stringOfN(length, Gen.oneOf(dnaCharsArrayMixedCase))
  } yield x

  def dnaStrings(minLen: Int): Gen[NTSeq] = dnaStrings(minLen, 200)

  val dnaStrings: Gen[NTSeq] = dnaStrings(1)

  def seedMaskSpaces(m: Int): Gen[SeqID] = Gen.choose(0, m / 2)
  def withSpacedSeed(p: MinimizerPriorities, spaces: Int): MinimizerPriorities =
    if (spaces == 0 || spaces > p.width / 2) p else SpacedSeed(spaces, p)

  def minimizerPriorities(m: Int): Gen[MinimizerPriorities] = {
    val DEFAULT_TOGGLE_MASK = 0xe37e28c4271b5a2dL
    val mp = if (m <= 10) {
      //These are expensive and large to generate so we use a lookup table
      Gen.oneOf(List(Testing.minTable(m), RandomXOR(m, DEFAULT_TOGGLE_MASK, canonical = true)))
    } else {
      Gen.oneOf(List(RandomXOR(m, DEFAULT_TOGGLE_MASK, canonical = true)))
    }
    for {
      x <- mp
      s <- seedMaskSpaces(m)
    } yield withSpacedSeed(x, s)
  }

  //The standard Shrink[String] will shrink the characters into non-ACTG chars, which we do not want
  implicit def shrinkNTSeq: Shrink[NTSeq] = Shrink { s =>
    implicit val shrinkChar: Shrink[Char] = Shrink.shrinkAny //do not shrink the chars in the string
    shrinkContainer[List,Char].shrink(s.toList).map(_.mkString)
  }

  val ks: Gen[Int] = ks(1)
  def ks(min: Int): Gen[Int] = Gen.choose(min, 91).filter(_ % 2 == 1)
  val ms: Gen[Int] = Gen.choose(1, 63)
  def ms(k: Int): Gen[Int] = Gen.choose(1, k)

  def mAndKPairs: Gen[(Int, Int)] =
    for {
      k <- ks
      m <- ms(k)
    } yield (m, k)

  /** Shrink m and k while maintaining the invariants we expect from them */
  implicit def shrinkMAndK: Shrink[(Int, Int)] =
    Shrink { case (t1,t2) =>
      shrink(t1).filter(_ >= 1).map((_,t2)) append
        shrink(t2).filter(_ >= t1).map((t1,_))
    }

  val abundances: Gen[Int] = Gen.choose(1, 10000)
  def encodedSupermers(minLen: Int): Gen[NTBitArray] = dnaStrings(minLen, 200).map(x => NTBitArray.encode(x))

  def encodedMinimizers(m: Int): Gen[Long] = Gen.choose(Long.MinValue, Long.MaxValue).
    map(x => x & (-1L >>> (64 - 2 * m)))

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

  val indexBuckets = 10
  /** Generate a random index with 10 buckets and a bogus splitter */
  def index(k: Int, m: Int)(implicit s: SparkSession): Gen[Index] = {
    import s.sqlContext.implicits._
    val splitter = MinSplitter(RandomXOR(m, 0, false), k) //dummy splitter, not used
    val params = IndexParams(s.sparkContext.broadcast(splitter), indexBuckets, "")
    Gen.containerOfN[List, ReducibleBucket](indexBuckets, reducibleBucket(k)).map(bs => {
      new Index(params, bs.zipWithIndex.map(x => x._1.copy(id = x._2)).toDS)
    })
  }
}

