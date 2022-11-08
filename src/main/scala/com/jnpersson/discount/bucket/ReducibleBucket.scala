/*
 * This file is part of Discount. Copyright (c) 2022 Johan Nyström-Persson.
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

package com.jnpersson.discount.bucket

import com.jnpersson.discount.hash.BucketId
import com.jnpersson.discount.util._
import com.jnpersson.discount.{Abundance, bucket}

import scala.collection.mutable.ArrayBuilder


/**
 * A bucket that maintains some number of super-mers and associated tags (e.g. counts)
 * of each k-mer.
 * @param id The ID/encoded minimizer of this bucket
 * @param supermers Encoded super-mers in this bucket
 * @param tags Tags (e.g. counts) of each k-mer in a 2D layout, rows corresponding to super-mers
 *             and columns corresponding to offsets within each super-mer.
 */
abstract class KmerBucket(id: BucketId, supermers: Array[ZeroNTBitArray],
                          tags: Array[Array[Tag]]) {

  def writeToTable(k: Int, forwardOnly: Boolean, sort: Boolean): KmerTable = {
    val provider = new TagProvider {
      def tagWidth = 2
      override def writeForRowCol(row: Tag, col: Tag, to: KmerTableBuilder): Unit = {
        to.addLong(row.toLong << 32 | col.toLong)
        to.addLong(tags(row)(col).toLong)
      }
    }
    KmerTable.fromSupermers(supermers, k, forwardOnly, sort, provider)
  }

  def writeToSortedTable(k: Int, forwardOnly: Boolean): KmerTable =
    writeToTable(k, forwardOnly, true)

}

object ReducibleBucket {

  /** Construct a ReducibleBucket with a counting (addition) reducer. Compacting will be performed. */
  def countingCompacted(id: BucketId, supermers: Array[ZeroNTBitArray], k: Int): ReducibleBucket = {
    val abundances = Arrays.fillNew(supermers.length, 1L)
    countingCompacted(id, supermers, abundances, k, filterOrientation = false)
  }

  /** Construct a ReducibleBucket with a counting (addition) reducer and the given abundances. */
  def countingCompacted(id: BucketId, supermers: Array[ZeroNTBitArray], abundances: Array[Abundance],
                        k: Int, filterOrientation: Boolean): ReducibleBucket = {
    val countTags = supermers.indices.toArray.map(i => {
      //Set the count of each k-mer to the abundance of the supermer
      //Note forced conversion from Long to Int! Limits counts to Int.MaxValue
      Arrays.fillNew(supermers(i).size - (k - 1), abundances(i).toInt)
    })
    ReducibleBucket(id, supermers, countTags).compact(Reducer.forK(k, filterOrientation))
  }

  /**
   * Intersection of two buckets.
   * The buckets must already have been compacted prior to calling this method (each k-mer
   * must occur only once per bucket with tag > 0)
   *
   * @param a Bucket 1
   * @param b Bucket 2
   * @param k Length of k-mers
   * @param reduceType
   * @return
   */
  def intersectCompact(a: ReducibleBucket, b: ReducibleBucket,
                       k: Int, reduceType: Reducer.Type): ReducibleBucket = {
    val reducer = Reducer.forK(k, false, reduceType)
    val supermers = a.supermers ++ b.supermers
    val tags = a.tags ++ b.tags
    ReducibleBucket(a.id, supermers, tags).compact(reducer)
  }

  def mergeCompact(b1: Option[ReducibleBucket], b2: Option[ReducibleBucket], k: Int,
                   reduceType: Reducer.Type): ReducibleBucket = {
    val reducer = Reducer.forK(k, false, reduceType)
    (b1, b2) match {
      case (Some(a), Some(b)) => a.merge(b, reducer)
      case (Some(a), _) => a
      case (_, Some(b)) => b
      case _ => throw new Exception("Can't merge two null CountingBuckets")
    }
  }
}

/**
 * A k-mer bucket that reduces (combines) identical k-mers using a supplied method,
 * removing redundant super-mers in the process to keep the bucket compact.
 * @param id The minimizer/ID of this bucket
 * @param supermers Super-mers containing the k-mers of this bucket. Some might not actually be present in the bucket
 *                  (the super-mers may have gaps) and the tags define which are present.
 *                  This arranges the k-mers in a 2D grid where rows identify the super-mer and columns
 *                  identify the offset in the super-mer. (Super-mers may however have different lengths)
 * @param tags Tags for each k-mer, for example k-mer counts in the case of k-mer counting. These follow the 2D
 *             coordinate scheme described above.
 */
final case class ReducibleBucket(id: BucketId, supermers: Array[ZeroNTBitArray],
                                 tags: Array[Array[Int]]) extends KmerBucket(id, supermers, tags) {

  def merge(other: ReducibleBucket, reducer: Reducer): ReducibleBucket = {
    ReducibleBucket(id, supermers ++ other.supermers, tags ++ other.tags).compact(reducer)
  }

  def compact(reducer: Reducer): ReducibleBucket = {
    val n = KmerTable.longsForK(reducer.k)
    val rowColOffset = n
    val tagOffset = reducer.tagOffset
    val newTags = tags.map(ts => Arrays.fillNew(ts.length, reducer.zeroValue))
    val reduced = reduceKmers(reducer)
    val kmers = reduced.kmers
    for {
      i <- reduced.indices
    } {
      //tags for non-kept k-mers will remain at 0
      if (reducer.shouldKeep(reduced, i)) {
        val row = (kmers(rowColOffset)(i) >> 32).toInt
        val col = (kmers(rowColOffset)(i) & Int.MaxValue).toInt
        newTags(row)(col) = kmers(tagOffset)(i).toInt
      }
    }

    val remainingMers = new ArrayBuilder.ofRef[ZeroNTBitArray]()
    val remainingTags = new ArrayBuilder.ofRef[Array[Int]]()
    remainingMers.sizeHint(supermers.length)
    remainingTags.sizeHint(supermers.length)

    for {
      i <- supermers.indices
      if reducer.hasNonZeroTag(newTags(i))
    } {
      remainingMers += supermers(i)
      remainingTags += newTags(i)
    }
    bucket.ReducibleBucket(id, remainingMers.result(), remainingTags.result())
  }

  def reduceKmers(reducer: Reducer): KmerTable = {
    val table = writeToSortedTable(reducer.k, reducer.forwardOnly)
    val it = table.indexIterator.buffered
    while (it.hasNext) {
      val thisKmer = it.next()
      while (it.hasNext && table.compareKmers(thisKmer, table, it.head) == 0) {
        reducer.reduceEqualKmers(table, thisKmer, it.next())
      }
    }
    table
  }
}
