/*
 * This file is part of Discount. Copyright (c) 2019-2024 Johan Nyström-Persson.
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

package com.jnpersson.kmers.util


import com.jnpersson.kmers.minimizer._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.jnpersson.kmers.TestGenerators._
import com.jnpersson.kmers.util.KmerTable.BuildParams
import org.scalatest.matchers.should.Matchers._

class KmerTableProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  def decode(data: Array[Long], k: Int) =
    NTBitArray.longsToString(data, 0, k)

  test("inserted k-mers can be correctly extracted") {
    forAll(dnaStrings, ks) { (x, k) =>
      whenever(k <= x.length && k >= 1 && x.nonEmpty) {
        val enc = NTBitArray.encode(x)
        val bpar = BuildParams(k, Both, true)
        val table = KmerTable.fromSegment(enc, bpar)
        val kmers = x.sliding(k)
        //Check that the data of each k-mer is the same
        table.toList.map(decode(_, k)).sorted should equal(kmers.toList.sorted)
      }
    }
  }
}
