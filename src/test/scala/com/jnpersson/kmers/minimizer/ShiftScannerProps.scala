/*
 * This file is part of Discount. Copyright (c) 2019-2026 Johan Nyström-Persson.
 *
 *  Discount is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Discount is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers.minimizer

import com.jnpersson.kmers.Testing
import com.jnpersson.kmers.util.{DNAHelpers, NTBitArray}
import org.scalacheck.Properties
import org.scalacheck.Prop._

class ShiftScannerProps extends Properties("ShiftScanner") {
  import com.jnpersson.kmers.TestGenerators._

  property("Find all m-mers") =
    forAll(ms(10)) { m =>
      forAll(dnaStringsMixedCase(m, 200)) { x =>
        (m <= x.size) ==> {
          // These minTable minimizers permit every m-mer to be a minimizer
          val space = Testing.minTable(m)
          val scanner = space.scanner
          val expected = x.sliding(m).toList.map(NTBitArray.encode)

          val actual = scanner.allMatches(x)._2.bitArraySeq.drop(m - 1) // first m-1 positions can't match
          val actualValid = scanner.allMatches(x)._2.validBitArrayIterator.toList

          val enc = NTBitArray.encode(x)
          val rcExpected = expected.map(_.reverseComplement).reverse
          val actualRc = scanner.allMatches(enc, true)._2.drop(m - 1) // first m-1 positions can't match
          val actualRcValid = scanner.allMatches(enc, true)._2.validBitArrayIterator.toList

          (actual == expected) :| "bitArraySeq matches expected sliding motifs" &&
            (actualValid == expected) :| "validBitArrayIterator matches expected sliding motifs" &&
            (actualRc == rcExpected) :| "reverse bitArraySeq matches expected sliding motifs" &&
            (actualRcValid == rcExpected) :| "reverse validBitArrayIterator matches expected sliding motifs"
        }
      }
    }

  property("Encoding of NT sequence") =
    forAll(ms(10)) { m =>
      forAll(dnaStringsMixedCase(m, 200)) { x =>
        val space = Testing.minTable(m)
        val scanner = space.scanner
        (scanner.allMatches(x)._1.toString == x.toUpperCase()) :| "encoded sequence matches input"
      }
    }
}
