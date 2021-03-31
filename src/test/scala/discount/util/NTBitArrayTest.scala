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

package discount.util

import discount.util.BitRepresentation
import org.scalacheck.{Gen, Prop, Properties}
import Prop._
import discount.NTSeq

import java.nio.ByteBuffer

object TestGenerators {
  import BitRepresentation._
  val dnaStrings: Gen[NTSeq] = for {
    length <- Gen.choose(1, 100)
    chars = (1 to length).map(x => twobitToChar(twobits((Math.random * 4).toInt)))
    x = new String(chars.toArray)
  } yield x

  val ks = Gen.choose(2, 90)

}

class NTBitArrayTest extends Properties("NTBitArray") {
  import TestGenerators._

  property("length") = forAll(dnaStrings) { x =>
    NTBitArray.encode(x).size == x.length
  }

  property("decoding") = forAll(dnaStrings) { x =>
    NTBitArray.encode(x).toString == x
  }

  property("copyPart identity") = forAll(dnaStrings) { x =>
    val enc = NTBitArray.encode(x)
    val buf = enc.partAsLongArray(0, enc.size)
    java.util.Arrays.equals(buf, enc.data)
  }

  property("k-mers length") = forAll(dnaStrings, ks) { (x, k) =>
    (k <= x.length) ==> {
      val kmers = NTBitArray.encode(x).kmersAsLongArrays(k, false).toArray
      kmers.size == (x.length - (k - 1))
    }
  }

  property("k-mers data") = forAll(dnaStrings, ks) { (x, k) =>
    (k <= x.length && k >= 1 && x.length >= 1) ==> {
      val kmers = NTBitArray.encode(x).kmersAsLongArrays(k, false).toArray
      val bb = ByteBuffer.allocate(32)
      val sb = new StringBuilder
      val kmerStrings = kmers.map(km => NTBitArray.longsToString(bb, sb, km, 0, k))
      kmerStrings.toList == x.sliding(k).toList
    }
  }
}
