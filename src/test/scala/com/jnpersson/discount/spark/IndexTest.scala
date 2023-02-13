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

package com.jnpersson.discount.spark

import com.jnpersson.discount.bucket.BucketStats
import com.jnpersson.discount.{Lexicographic, Testing}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util

class IndexTest extends AnyFunSuite with Matchers with SparkSessionTestWrapper {
  implicit val s = spark

  val m = 9

  def makeIndex(input: String, k: Int): Index = {
    val discount = new Discount(k, Bundled, m)
    discount.index(input)
  }

  //Check that the index has the expected overall k-mer stats
  def checkIndexStats(i: Index, expectedStats: BucketStats): Unit = {
    val all = i.stats().collect().reduce(_ merge _)
    all.equalCounts(expectedStats) should be(true)
  }

  //Check that two indexes have exactly the same k-mers and counts
  def checkIndexEquality(i1: Index, i2: Index): Unit = {
    val data1 = i1.counted().counts.sort("_1").collect()
    val data2 = i2.counted().counts.sort("_1").collect()
    for { i <- data1.indices } {
      assert(util.Arrays.equals(data1(i)._1, data2(i)._1) &&
        data1(i)._2 == data2(i)._2)
    }
  }

  test("create, write, read back") {
    val k = 31
    //TODO find a better way to configure temp dir for tests
    val location = "/tmp/testData/10k_test"

    val index = makeIndex("testData/SRR094926_10k.fasta", k)
    val all = index.stats().collect().reduce(_ merge _)
    all.equalCounts(Testing.correctStats10k31) should be(true)

    index.write(location)
    val i2 = Index.read(location)
    checkIndexStats(i2, Testing.correctStats10k31)
  }

  test("reorder minimizers") {
    val k = 31

    val i1 = makeIndex("testData/SRR094926_10k.fasta", k).cache()
    val ordering2 = new Discount(k, Bundled, m, Lexicographic).
      getSplitter(None, None)
    val i2 = i1.changeMinimizerOrdering(spark.sparkContext.broadcast(ordering2)).cache()
    checkIndexStats(i2, Testing.correctStats10k31)
    checkIndexEquality(i1, i2)
  }

  test("self union") {
    val k = 31
    val i1 = makeIndex("testData/SRR094926_10k.fasta", k)
    checkIndexStats(i1.union(i1, Rule.Max), Testing.correctStats10k31)
  }

  test("self intersect") {
    val k = 31
    val i1 = makeIndex("testData/SRR094926_10k.fasta", k)
    checkIndexStats(i1.intersect(i1, Rule.Max), Testing.correctStats10k31)
  }
}
