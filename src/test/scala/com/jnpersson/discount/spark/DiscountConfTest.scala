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

package com.jnpersson.discount.spark

import com.jnpersson.kmers.SparkSessionTestWrapper
import com.jnpersson.kmers.minimizer.Lexicographic
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Test for Discount CLI parsing. */
class DiscountConfTest extends AnyFunSuite with SparkSessionTestWrapper with Matchers {
  implicit val sp = spark

  /** Test a set of command line arguments to Discount. */
  def testArgs(args: String) = {
    val s = new DiscountConf(args.trim.split("\\s+"))
    s.verify()
    s
  }

  test("basic") {
    val conf = testArgs("-k 31 -m 10 -o lexicographic input.fasta stats")
    conf.k() should equal(31)
    conf.minimizerWidth() should equal(10)
    conf.ordering() should equal(Lexicographic)
    conf.inputFiles() should equal(List("input.fasta"))
  }

  test("various cases") {
    testArgs("-k 35 10M.fasta stats -o 10M_stats.txt")
    testArgs("-i 10M_35 count -o counted_kmers")
    testArgs("--method pregrouped -k 55 /path/to/data.fastq stats")
    testArgs("-i 10M_35 intersect -i 10M_35_2 -r max -o 35_int")
    testArgs("-i index1_path union -r max -i index2_path index3_path -o union3_path")
  }

}