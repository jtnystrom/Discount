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

package com.jnpersson.discount.hash

import com.jnpersson.discount.TestGenerators._
import org.scalatest.funsuite._
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class MinSplitterProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import com.jnpersson.discount.TestGenerators.shrinkMAndK

  test("splitting preserves all data") {
    forAll(mAndKPairs) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(k)) { (pri, x) =>
        whenever(k <= x.size) {
          val extractor = MinSplitter(pri, k)
          val encoded = extractor.splitEncode(x).toList
          val regions = encoded.map(_._3.toString)

          (regions.head + regions.tail.map(_.substring(k - 1)).mkString("")) should equal(x)

          for {(_, _, ntseq, location) <- encoded} {
            x.substring(location.toInt, location.toInt + ntseq.size) should equal(ntseq.toString)
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
          val expected = regions.map(r => scanner.allMatches(r._3)._2.validBitArrayIterator.min)
          val results = regions.map(_._2)

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


}
