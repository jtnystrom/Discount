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

package discount.util

import discount.Abundance

import scala.collection.mutable

object KmerTable {
  def longsForK(k: Int): Int = {
    if (k % 32 == 0) {
      k / 32
    } else {
      k / 32 + 1
    }
  }

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
                   forwardOnly: Boolean, sort: Boolean = true): KmerTable = {

    //Theoretical max #k-mers in a perfect super-mer is (2 * k - m).
    //On average a super-mer is about 10 k-mers in cases we have seen.
    val estimatedSize = segments.size * 15
    val builder = new KmerTableBuilder(longsForK(k), estimatedSize)

    for {s <- segments} {
      s.writeKmersToBuilder(builder, k, forwardOnly)
    }
    builder.result(sort)
  }
}

/**
 * Construct a new KmerTableBuilder.
 * @param n Width of k-mers (in longs, e.g. ceil(k/32))
 * @param sizeEstimate Estimated number of k-mers that will be inserted
 */
final class KmerTableBuilder(n: Int, sizeEstimate: Int) {
  val builders = Array.fill(n)(new mutable.ArrayBuilder.ofLong)
  for (b <- builders) {
    b.sizeHint(sizeEstimate)
  }

  var writeColumn = 0

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

  def result(sort: Boolean): KmerTable = {
    val r = builders.map(_.result())
    if (r(0).nonEmpty && sort) {
      LongArrays.radixSort(r)
    }
    n match {
      case 1 => new KmerTable1(r)
      case 2 => new KmerTable2(r)
      case 3 => new KmerTable3(r)
      case _ => new KmerTableN(r, n)
    }
  }
}

/**
 * k-mers stored in column-major format rather than row-major.
 * The first k-mer is stored in kmers(0)(0), kmers(1)(0), ... kmers(n)(0);
 * the second in kmers(0)(1), kmers(1)(1)... kmers(n)(1) and so on.
 * This layout enables fast radix sort.
 * The KmerTable is optionally sorted by construction (by KmerTableBuilder).
 * @param kmers
 */
abstract class KmerTable(kmers: Array[Array[Long]]) extends Iterable[Array[Long]] {
  override val size = kmers(0).length

  /**
   * Test whether the k-mer at position i is equal to the given one.
   * @param i
   * @param kmer
   * @return
   */
  def equalKmers(i: Int, kmer: Array[Long]): Boolean

  /**
   * Copy the k-mer at position i to a new long array.
   * @param i
   * @return
   */
  def copyKmer(i: Int): Array[Long]

  /**
   * Obtain distinct k-mers and their counts. Requires that the KmerTable was sorted at construction time.
   * @return
   */
  def countedKmers: Iterator[(Array[Long], Abundance)] = new Iterator[(Array[Long], Abundance)] {
    var i = 0
    val len = KmerTable.this.size
    def hasNext: Boolean = i < len

    def next: (Array[Long], Abundance) = {
      val lastKmer = copyKmer(i)
      var count = 1L
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

  def iterator: Iterator[Array[Long]] = new Iterator[Array[Long]] {
    var i = 0
    val len = KmerTable.this.size
    def hasNext: Boolean = i < len

    def next: Array[Long] = {
      val r = copyKmer(i)
      i += 1
      r
    }
  }
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