/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers.minimizer

import com.jnpersson.kmers.Testing
import com.jnpersson.kmers.util.{DNAHelpers, NTBitArray}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ShiftScannerProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import com.jnpersson.kmers.TestGenerators._

  test("Find all m-mers") {
    forAll(ms(10)) { m =>
      forAll(dnaStringsMixedCase(m, 200)) { x =>
        whenever(m <= x.size) {

          //These minTable minimizers permit every m-mer to be a minimizer
          val space = Testing.minTable(m)
          val scanner = space.scanner
          val expected = x.sliding(m).toList.map(_.toUpperCase())

          def priorityToString(pri: NTBitArray) =
            space.motifArray(pri.toInt).toString

          scanner.allMatches(x)._2.bitArraySeq.drop(m - 1). //first m-1 positions can't have an m-length match
            map(priorityToString) should equal(expected)

          scanner.allMatches(x)._2.validBitArrayIterator.
            map(priorityToString).toList should equal(expected)

          val enc = NTBitArray.encode(x)
          val rcString = DNAHelpers.reverseComplement(x)
          val rcExpected = rcString.sliding(m).toList.map(_.toUpperCase())

          scanner.allMatches(enc, true)._2.drop(m - 1). //first m-1 positions can't have an m-length match
            map(priorityToString) should equal(rcExpected)
          scanner.allMatches(enc, true)._2.validBitArrayIterator.
            map(priorityToString).toList should equal(rcExpected)
        }
      }
    }
  }

  test("Encoding of NT sequence") {
    forAll(ms(10)) { m =>
      forAll(dnaStringsMixedCase(m, 200)) { x =>
        val space = Testing.minTable(m)
        val scanner = space.scanner
        scanner.allMatches(x)._1.toString should equal(x.toUpperCase())
      }
    }
  }
}
