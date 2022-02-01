/*
 * This file is part of Discount. Copyright (c) 2021 Johan Nyström-Persson.
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

import scala.collection.{Seq, mutable}
import com.jnpersson.discount.hash.MotifSpace
import com.jnpersson.discount.util.{BitRepresentation, NTBitArray, ZeroNTBitArray}
import org.scalacheck.{Gen, Shrink}

object Testing {
  //Cache these so that we can test many properties efficiently
  //without allocating this big object each time
  private var spaces = mutable.Map[Int, MotifSpace]()
  def motifSpace(m: Int): MotifSpace = synchronized {
    spaces.get(m) match {
      case Some(s) => s
      case _ =>
        val space = MotifSpace.ofLength(m, false)
        spaces(m) = space
        space
    }
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

  //The standard Shrink[String] will shrink the characters into non-ACTG chars, which we do not want
  implicit def shrinkNTSeq: Shrink[NTSeq] = Shrink { s =>
    Stream.cons(s.substring(0, s.length - 1),
      (1 until s.length).map(i => s.substring(0, i) + s.substring(i + 1, s.length)).toStream
    )
  }

  val ks: Gen[Int] = Gen.choose(1, 50)
  val ms: Gen[Int] = Gen.choose(1, 10)

  val dnaLetterTwobits: Gen[Byte] = Gen.choose(0, 3).map(x => twobits(x))
  val dnaLetters: Gen[Char] = dnaLetterTwobits.map(x => twobitToChar(x))

  val abundances: Gen[Int] = Gen.choose(1, 100)

  def encodedSupermers(minLen: Int): Gen[ZeroNTBitArray] = dnaStrings(minLen, 100).map(x => NTBitArray.encode(x))

  def fastaSequences(partLen: Int, minParts: Int, maxParts: Int): Gen[NTSeq] = for {
    n <- Gen.choose(minParts, maxParts)
    dna <- Gen.listOfN(n, dnaStrings(partLen, partLen))
    str = dna.mkString("\n")
  } yield str
}