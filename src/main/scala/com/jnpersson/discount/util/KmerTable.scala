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

package com.jnpersson.discount.util

import com.jnpersson.discount.Abundance

import scala.collection.mutable

object KmerTable {
  /** Number of longs required to represent a k-mer of length k */
  def longsForK(k: Int): Int = {
    if (k % 32 == 0) {
      k / 32
    } else {
      k / 32 + 1
    }
  }

  /** Obtain a new KmerTableBuilder */
  def builder(k: Int, sizeEstimate: Int = 100, extraItems: Int = 0): KmerTableBuilder =
    new KmerTableBuilder(longsForK(k) + extraItems, sizeEstimate)

  /** Obtain a KmerTable from a single segment/superkmer */
  def fromSegment(segment: NTBitArray, k: Int, forwardOnly: Boolean, sort: Boolean = true): KmerTable =
    fromSegments(List(segment), k, forwardOnly, sort)

  /**
   * Construct a KmerTable from super k-mers.
   * @param segments Super k-mers
   * @param k
   * @param forwardOnly Whether to filter out k-mers with reverse orientation
   * @param sort Whether to sort the k-mers
   * @return
   */
  def fromSegments(segments: Iterable[NTBitArray], k: Int,
                   forwardOnly: Boolean, sort: Boolean = true): KmerTable =
    fromSupermers(segments, k, forwardOnly, sort, 0, (row, col) => Array.empty)

  /**
   * Write super-mers as k-mers, along with tag data, to a new KmerTable.
   * @param supermers
   * @param k k
   * @param forwardOnly Whether to filter out k-mers with reverse orientation
   * @param sort Whether to sort the result
   * @param tagLength The length (in longs) of the tag data for each k-mer
   * @param tagData Extra (tag) data for the given row and column, to be appended after the k-mer data
   * @return
   */
  def fromSupermers(supermers: Iterable[NTBitArray], k: Int, forwardOnly: Boolean,
                    sort: Boolean, tagLength: Int, tagData: (Int, Int) => Array[Long]): KmerTable = {

    //Theoretical max #k-mers in a perfect super-mer is (2 * k - m).
    val estimatedSize = supermers.size * 20
    val n = KmerTable.longsForK(k)
    val builder = new KmerTableBuilder(n + tagLength, estimatedSize)
    for { (s, row) <- supermers.zipWithIndex } {
      s.writeKmersToBuilder(builder, k, forwardOnly, col => tagData(row, col))
    }
    builder.result(sort)
  }
}

/**
 * Builder for k-mer tables. K-mers are built by gradually adding longs in order.
 * @param n Width of k-mers (in longs, e.g. ceil(k/32)). Can include extra longs used to annotate k-mers with additional information
 * @param sizeEstimate Estimated number of k-mers that will be inserted
 */
final class KmerTableBuilder(n: Int, sizeEstimate: Int) {
  private val builders = Array.fill(n)(new mutable.ArrayBuilder.ofLong)
  for (b <- builders) {
    b.sizeHint(sizeEstimate)
  }

  private var writeColumn = 0

  /**
   * Add a single long value. Calling this method n times adds a single k-mer to the table.
   */
  def addLong(x: Long): Unit = {
    builders(writeColumn) += x
    writeColumn += 1
    if (writeColumn == n) {
      writeColumn = 0
    }
  }

  def addLongs(xs: Array[Long]): Unit = {
    var i = 0
    while (i < xs.length) {
      addLong(xs(i))
      i += 1
    }
  }

  /**
   * Construct a k-mer table that contains all the inserted k-mers.
   * After calling this method, this builder is invalid and should be discarded.
   * @param sort Whether the k-mers should be sorted.
   * @return
   */
  def result(sort: Boolean): KmerTable = {
    val r = builders.map(_.result())
    if (r(0).nonEmpty && sort) {
      LongArrays.radixSort(r)
    }
    n match {
      case 1 => new KmerTable1(r)
      case 2 => new KmerTable2(r)
      case 3 => new KmerTable3(r)
      case 4 => new KmerTable4(r)
      case _ => new KmerTableN(r, n)
    }
  }
}

/**
 * A collection of k-mers stored in column-major format rather than row-major.
 * The first k-mer is stored in kmers(0)(0), kmers(1)(0), ... kmers(n)(0);
 * the second in kmers(0)(1), kmers(1)(1)... kmers(n)(1) and so on.
 * This layout enables fast radix sort.
 * The KmerTable is optionally sorted by construction (by KmerTableBuilder).
 * Each k-mer may contain additional annotation data in longs following the sequence data itself.
 * @param kmers
 */
abstract class KmerTable(val kmers: Array[Array[Long]]) extends Iterable[Array[Long]] {
  override val size = kmers(0).length

  /**
   * Test whether the k-mer at position i is equal to the given one.
   *
   * @param i
   * @param kmer
   * @return
   */
  def equalKmers(i: Int, kmer: Array[Long]): Boolean

  /**
   * Copy the k-mer at position i to a new long array.
   *
   * @param i
   * @return
   */
  def copyKmer(i: Int): Array[Long]

  /**
   * Obtain distinct k-mers and their counts. Requires that the KmerTable was sorted at construction time.
   *
   * @return
   */
  def countedKmers: Iterator[(Array[Long], Abundance)] = new Iterator[(Array[Long], Abundance)] {
    var i = 0
    val len = KmerTable.this.size

    def hasNext: Boolean = i < len

    def next: (Array[Long], Abundance) = {
      val lastKmer = copyKmer(i)
      var count: Abundance = 1
      if (!hasNext) {
        return (lastKmer, count)
      }
      i += 1
      while (i < len && equalKmers(i, lastKmer)) {
        count += 1
        i += 1
      }

      (lastKmer, count)
    }
  }

  /** Iterator with k-mer data only */
  def iterator: Iterator[Array[Long]] =
    Iterator.range(0, KmerTable.this.size).map(i => copyKmer(i))

  /** Iterator including both k-mer data and tag data */
  def iteratorWithTags: Iterator[Array[Long]] =
    Iterator.range(0, KmerTable.this.size).map(i =>
      Array.tabulate(kmers.length)(x => kmers(x)(i)))
}

/**
 * Specialized KmerTable for n = 1 (k <= 32)
 * @param kmers
 */
final class KmerTable1(kmers: Array[Array[Long]]) extends KmerTable(kmers) {
  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    kmers(0)(i) == kmer(0)
  }

  def copyKmer(i: Int): Array[Long] = {
    Array(kmers(0)(i))
  }
}

/**
 * Specialized KmerTable for n = 2 (k <= 64)
 * @param kmers
 */
final class KmerTable2(kmers: Array[Array[Long]]) extends KmerTable(kmers) {
  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    kmers(0)(i) == kmer(0) &&
      kmers(1)(i) == kmer(1)
  }

  def copyKmer(i: Int): Array[Long] = {
    Array(kmers(0)(i), kmers(1)(i))
  }
}

/**
 * Specialized KmerTable for n = 3 (k <= 96)
 * @param kmers
 */
final class KmerTable3(kmers: Array[Array[Long]]) extends KmerTable(kmers) {
  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    kmers(0)(i) == kmer(0) &&
      kmers(1)(i) == kmer(1) &&
      kmers(2)(i) == kmer(2)
  }

  def copyKmer(i: Int): Array[Long] = {
    Array(kmers(0)(i), kmers(1)(i), kmers(2)(i))
  }
}

final class KmerTable4(kmers: Array[Array[Long]]) extends KmerTable(kmers) {
  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    kmers(0)(i) == kmer(0) &&
      kmers(1)(i) == kmer(1) &&
      kmers(2)(i) == kmer(2) &&
      kmers(3)(i) == kmer(3)
  }

  def copyKmer(i: Int): Array[Long] = {
    Array(kmers(0)(i), kmers(1)(i), kmers(2)(i), kmers(3)(i))
  }
}

/**
 * General KmerTable for any value of n
 * @param kmers
 */
final class KmerTableN(kmers: Array[Array[Long]], n: Int) extends KmerTable(kmers) {
  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    var j = 0
    while (j < n) {
      if (kmers(j)(i) != kmer(j)) return false
      j += 1
    }
    true
  }

  def copyKmer(i: Int): Array[Long] =
    Array.tabulate(n)(j => kmers(j)(i))
}