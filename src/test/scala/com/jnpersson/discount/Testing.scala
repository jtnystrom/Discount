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

package com.jnpersson.discount

import com.jnpersson.discount.bucket.{BucketStats, ReducibleBucket}

import scala.collection.mutable
import com.jnpersson.discount.hash.{MinTable, MinimizerPriorities, RandomXOR}
import com.jnpersson.discount.util.{BitRepresentation, KmerTable, NTBitArray, ZeroNTBitArray}
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
  implicit def shrinkNTSeq: Shrink[NTSeq] = Shrink { s =>
    Stream.cons(s.substring(0, s.length - 1),
      (1 until s.length).map(i => s.substring(0, i) + s.substring(i + 1, s.length)).toStream
    )
  }

  val ks: Gen[Int] = Gen.choose(1, 55)
  val ms: Gen[Int] = Gen.choose(1, 31)

  val dnaLetterTwobits: Gen[Byte] = Gen.choose(0, 3).map(x => twobits(x))
  val dnaLetters: Gen[Char] = dnaLetterTwobits.map(x => twobitToChar(x))

  val abundances: Gen[Int] = Gen.choose(1, 100)

  def encodedSupermers(minLen: Int): Gen[ZeroNTBitArray] = dnaStrings(minLen, 100).map(x => NTBitArray.encode(x))

  def encodedMinimizers(m: Int): Gen[Long] = Gen.choose(Long.MinValue, Long.MaxValue).
    map(x => x & (-1L >>> (64 - 2 * m)))

  def fastaSequences(partLen: Int, minParts: Int, maxParts: Int): Gen[NTSeq] = for {
    n <- Gen.choose(minParts, maxParts)
    dna <- Gen.listOfN(n, dnaStrings(partLen, partLen))
    str = dna.mkString("\n")
 } yield str

  def reducibleBuckets(k: Int): Gen[ReducibleBucket] = for {
    nSupermers <- Gen.choose(1, 10)
    supermers <- Gen.listOfN(nSupermers, encodedSupermers(k))
    tags = supermers.toArray.map(sm => (0 until sm.size - (k - 1)).map(x => 1).toArray)
  } yield ReducibleBucket(1, supermers.toArray, tags)
}

object KmerTableGenerators {
  import TestGenerators._

  def tagWidths: Gen[Int] = Gen.choose(0, 3)

  def encodedKmers(k: Int): Gen[Array[Long]] =
    dnaStrings(k, k).map(str => NTBitArray.encode(str).data)

  //Disable shrinking of k-mers
  implicit def shrinkEncodedKmers: Shrink[Array[Long]] = Shrink(s => Stream.empty)
  //Allow the list itself to shrink, but not the k-mers
  implicit def shrinkEncodedKmerList: Shrink[List[Array[Long]]] = Shrink.shrinkContainer
  implicit def shrinkEncodedKmerArray: Shrink[Array[Array[Long]]] = Shrink.shrinkContainer

  def kmerTags(tagWidth: Int): Gen[Array[Long]] =
    Gen.listOfN(tagWidth, Gen.long).map(_.toArray)

  private def sortedKmerTable(k: Int, tagWidth: Int, size: Int,
                              kmers: List[Array[Long]], tags: List[Array[Long]]): KmerTable = {
    val builder = KmerTable.builder(k, size, tagWidth)
    for {
      (km, tg) <- kmers zip tags
    } {
      builder.addLongs(km)
      builder.addLongs(tg)
    }
    builder.result(true)
  }

  /** Generate k-mer tables with tags */
  def kmerTables(k: Int, tagWidth: Int, size: Int): Gen[KmerTable] =
    kmerTables(k, tagWidth, size, Array())

  /** Generate k-mer tables with tags where a set of given k-mers are guaranteed to be included.
   * The final size will be size + includeKmers.length */
  def kmerTables(k: Int, tagWidth: Int, size: Int, includeKmers: Array[Array[Long]]): Gen[KmerTable] = {
    val fullSize = size + includeKmers.length
    for {
      kmers <- Gen.listOfN(size, encodedKmers(k))
      tags <- Gen.listOfN(fullSize, kmerTags(tagWidth))
      table = sortedKmerTable(k, tagWidth, fullSize, kmers ++ includeKmers, tags)
    } yield table
  }
}