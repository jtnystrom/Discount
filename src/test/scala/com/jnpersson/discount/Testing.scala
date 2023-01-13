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
import com.jnpersson.discount.bucket.{BucketStats, Reducer, ReducibleBucket, Tag}

import scala.collection.mutable
import com.jnpersson.discount.hash.{MinTable, MinimizerPriorities, RandomXOR}
import com.jnpersson.discount.util.{BitRepresentation, NTBitArray, ZeroNTBitArray}
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
}

object TestGenerators {
  import BitRepresentation._

  def dnaStrings(minLen: Int, maxLen: Int): Gen[NTSeq] = for {
    length <- Gen.choose(minLen, maxLen)
    chars <- Gen.listOfN(length, dnaLetters)
    x = new String(chars.toArray)
  } yield x

  val dnaStrings: Gen[NTSeq] = dnaStrings(1, 100)

  def minimizerPriorities(m: Int): Gen[MinimizerPriorities] = {
    val DEFAULT_TOGGLE_MASK = 0xe37e28c4271b5a2dL
    if (m <= 10) {
      Gen.oneOf(List(Testing.minTable(m), RandomXOR(m, DEFAULT_TOGGLE_MASK, canonical = true)))
    } else {
      Gen.oneOf(List(RandomXOR(m, DEFAULT_TOGGLE_MASK, canonical = true)))
    }

  }

  //The standard Shrink[String] will shrink the characters into non-ACTG chars, which we do not want
  implicit def shrinkNTSeq: Shrink[NTSeq] = Shrink.withLazyList { s =>
    //Note: should migrate Stream to LazyList when ScalaCheck supports it
    LazyList.cons(s.substring(0, s.length - 1),
      (1 until s.length).map(i => s.substring(0, i) + s.substring(i + 1, s.length)).to(LazyList)
    )
  }

  val ks: Gen[Int] = Gen.choose(1, 55)
  val ms: Gen[Int] = Gen.choose(1, 31)

  val dnaLetterTwobits: Gen[Byte] = Gen.choose(0, 3).map(x => twobits(x))
  val dnaLetters: Gen[Char] = dnaLetterTwobits.map(x => twobitToChar(x))

  val abundances: Gen[Int] = Gen.choose(1, 10000)
  def encodedSupermers(minLen: Int): Gen[ZeroNTBitArray] = dnaStrings(minLen, 100).map(x => NTBitArray.encode(x))

  def encodedMinimizers(m: Int): Gen[Long] = Gen.choose(Long.MinValue, Long.MaxValue).
    map(x => x & (-1L >>> (64 - 2 * m)))

  def kmerTags(n: Int): Gen[Array[Tag]] =
    Gen.listOfN(n, abundances).map(_.toArray)

  def kmerTags(sm: ZeroNTBitArray, k: Int): Gen[Array[Tag]] = kmerTags(sm.size - (k - 1))

  def kmerTags(sms: Array[ZeroNTBitArray], k: Int): Gen[collection.Seq[Array[Tag]]] =
    Gen.sequence(sms.map(sm => kmerTags(sm, k)))(Buildable.buildableSeq)

  def reducibleBuckets(k: Int): Gen[ReducibleBucket] = {
    val sumReducer = Reducer.configure(k, forwardOnly = false, intersect = false, Sum)
    for {
      nSupermers <- Gen.choose(1, 10)
      supermers <- Gen.listOfN(nSupermers, encodedSupermers(k)).map(_.toArray)
      tags <- kmerTags(supermers, k)
      b = ReducibleBucket(0, supermers, tags.toArray)
    } yield b.reduceCompact(sumReducer)
  }

  //Generate pairs of two buckets that have distinct super-mers and also common super-mers.
  //For the common super-mers, the tags (counts) need not be the same for the two buckets.
  def bucketPairsWithCommonKmers(k: Int): Gen[(ReducibleBucket, ReducibleBucket)] = {
    val sumReducer = Reducer.configure(k, forwardOnly = false, intersect = false, Sum)
    for {
      bucket1 <- reducibleBuckets(k)
      bucket2 <- reducibleBuckets(k)
      n <- Gen.choose(1, 10)
      commonSupermers <- Gen.listOfN(n, encodedSupermers(k)).map(_.toArray)
      tags1 <- kmerTags(commonSupermers, k)
      tags2 <- kmerTags(commonSupermers, k)
      bc1 = bucket1.appendAndCompact(ReducibleBucket(0, commonSupermers, tags1.toArray), sumReducer)
      bc2 = bucket2.appendAndCompact(ReducibleBucket(0, commonSupermers, tags2.toArray), sumReducer)
    } yield (bc1, bc2)
  }
}
