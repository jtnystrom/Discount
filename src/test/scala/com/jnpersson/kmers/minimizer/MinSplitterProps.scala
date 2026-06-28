/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.kmers.minimizer

import com.jnpersson.kmers.TestGenerators._
import com.jnpersson.kmers.util.NTBitArray
import org.scalatest.funsuite._
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class MinSplitterProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import com.jnpersson.kmers.TestGenerators.shrinkMAndK

  test("splitting preserves all data") {
    forAll(mAndKPairs) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(k)) { (pri, x) =>
        whenever(k <= x.size) {
          val extractor = MinSplitter(pri, k)
          val encoded = extractor.splitEncode(x).toList
          val supermers = encoded.map(_.nucleotides.toString)

          (supermers.head + supermers.tail.map(_.substring(k - 1)).mkString("")) should equal(x)

          for {Supermer(_, ntseq, location) <- encoded} {
            x.substring(location.toInt, location.toInt + ntseq.size) should equal(ntseq.toString)
          }
        }
      }
    }
  }

  test("adjacent minimizers are not identical") {
    forAll(mAndKPairs) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(k)) { (pri, x) =>
        whenever(k <= x.size) {
          val extractor = MinSplitter(pri, k)
          val encoded = extractor.splitEncode(x).map(_.rank.toList)

          for { pair <- encoded.sliding(2)
                if pair.length == 2 } {
            pair(0) should not equal pair(1)
          }
        }
      }
    }
  }

  test("extracted minimizers are minimal m-mers") {
    forAll(mAndKPairs) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(k)) { (pri, x) =>
        whenever(k <= x.size) {
          val extractor = MinSplitter(pri, k)
          val scanner = ShiftScanner(pri)
          val regions = extractor.splitEncode(x).toList

          //An improved version of this test would compare not only features but also the position of the motif
          val expected = regions.map(r => scanner.allMatches(r.nucleotides)._2.validBitArrayIterator.min.data.toList)
          val results = regions.map(_.rank.toList)

          results should equal(expected)
        }
      }
    }
  }

  test("too short sequences have no minimizers") {
    forAll(mAndKPairs) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(0, k - 1)) { (pri, x) =>
        val extractor = MinSplitter(pri, k)
        val regions = extractor.splitEncode(x).toList
        regions should be(empty)
      }
    }
  }

  test("Canonical priorities return the same minimizer for reverse complement") {
    forAll(ms) { case m =>
      forAll(minimizerPrioritiesCanonical(m), dnaStrings(m, m)) { (pri, x) =>
        val enc = NTBitArray.encode(x)
        val rc = enc.reverseComplement
        pri.priorityOf(enc) should equal(pri.priorityOf(rc))
      }
    }
  }

  test("Super-mers are invariant under reverse complement") {
    forAll(mAndKPairs) { case (m, k) =>
      forAll(minimizerPrioritiesCanonical(m), dnaStrings(k)) { (pri, x) =>
        whenever(k <= x.size) {
          val extractor = MinSplitter(pri, k)
          val encoded = NTBitArray.encode(x)
          val regions = extractor.splitRead(encoded, false).toList
          val rcRegions = extractor.splitRead(encoded, true).toList

          regions.map(_.rank.toList) should equal(rcRegions.map(_.rank.toList).reverse)
        }
      }
    }
  }

  test("splitRead and superkmerPositions return the same data") {
    forAll(mAndKPairs) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(k)) { (pri, x) =>
        whenever(k <= x.size) {
          val encoded = NTBitArray.encode(x)
          val extractor = MinSplitter(pri, k)
          val mins1 = extractor.splitRead(encoded).map(_.rank.toList).toList
          val mins2 = extractor.superkmerPositions(encoded).map(_.rank.toList).toList
          mins1 should equal(mins2)
        }
      }
    }
  }

}
