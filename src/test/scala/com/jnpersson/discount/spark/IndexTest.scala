/*
 * This file is part of Hypercut. Copyright (c) 2022 Johan Nyström-Persson.
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

  def makeIndex(input: String, location: String, k: Int, buckets: Int): Index = {
    val discount = new Discount(k, Bundled, m)
    discount.kmers("testData/SRR094926_10k.fasta").index
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
    val buckets = 20

    val index = makeIndex("testData/SRR094926_10k.fasta", location, k, buckets)
    val all = index.stats().collect().reduce(_ merge _)
    all.equalCounts(Testing.correctStats10k31) should be(true)

    index.write(location)
    val i2 = Index.read(location)
    checkIndexStats(i2, Testing.correctStats10k31)
  }

  test("reorder minimizers") {
    val location = "/tmp/testData/10k_test"
    val buckets = 20
    val k = 31

    val i1 = makeIndex("testData/SRR094926_10k.fasta", location, k, buckets).cache()
    val ordering2 = new Discount(k, Bundled, m, Lexicographic).
      getSplitter(None, None)
    val i2 = i1.changeMinimizerOrdering(spark.sparkContext.broadcast(ordering2)).cache()
    checkIndexStats(i2, Testing.correctStats10k31)
    checkIndexEquality(i1, i2)
  }
}
