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

package com.jnpersson.discount.hash

import com.jnpersson.discount.TestGenerators._
import com.jnpersson.discount.{Testing}
import org.scalatest.funsuite._
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class MinSplitterProps extends AnyFunSuite with ScalaCheckPropertyChecks {

  test("splitting preserves all data") {
    forAll(dnaStrings, ks, ms) { (x, k, m) =>
      whenever (1 <= m && m <= k && k <= x.length) {
        val space = Testing.motifSpace(m)
        val extractor = MinSplitter(space, k)
        val encoded = extractor.splitEncode(x).toList
        val regions = encoded.map(_._2.toString)

        (regions.head + regions.tail.map(_.substring(k - 1)).mkString("")) should equal(x)

        for { (motif, ntseq, location) <- encoded } {
          x.substring(location.toInt, location.toInt + ntseq.size) should equal(ntseq.toString)
        }
      }
    }
  }

  test("extracted minimizers are minimal m-mers") {
    forAll(dnaStrings, ks, ms) { (x, k, m) =>
      whenever (1 <= m && m <= k && k <= x.length) {
        val space = Testing.motifSpace(m)
        val extractor = MinSplitter(space, k)
        val scanner = space.scanner
        val regions = extractor.splitEncode(x).toList

        //An improved version of this test would compare not only features but also the position of the motif
        val expected = regions.map(r => scanner.allMatches(r._2.toString)._2.filter(_ != Motif.INVALID).
          sorted.head)
        val results = regions.map(_._1.features.rank)

        results should equal(expected)
      }
    }
  }

}
