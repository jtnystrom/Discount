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

package com.jnpersson.discount.util

import com.jnpersson.discount.Abundance
import com.jnpersson.discount.bucket.Reducer
import it.unimi.dsi.fastutil.longs.LongArrays

import scala.collection.mutable

/** Source of tags for a set of k-mers arranged in two dimensions,
 * where the row identifies a super-mer and the col identifies a k-mer position in the super-mer */
trait TagProvider {
  def tagWidth: Int

  /** Write tags for a specific row and column to the builder */
  def writeForRowCol(row: Int, col: Int, to: KmerTableBuilder): Unit
}

/** Source of tags for a single row (super-mer) of k-mers */
trait RowTagProvider {

  /** Write tags for a given column (k-mer) to the builder */
  def writeForCol(col: Int, to: KmerTableBuilder): Unit
}

case object EmptyRowTagProvider extends RowTagProvider {
  override def writeForCol(col: Int, to: KmerTableBuilder): Unit = {}
}

/** Wrap a TagProvider into a RowTagProvider by fixing the row */
case class NestedRowTagProvider(row: Int, inner: TagProvider) extends RowTagProvider {
  def writeForCol(col: Int, to: KmerTableBuilder): Unit =
    inner.writeForRowCol(row, col, to)
}


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
  def builder(k: Int, sizeEstimate: Int = 100, tagWidth: Int = 0): KmerTableBuilder =
    new KmerTableBuilder(longsForK(k) + tagWidth, tagWidth, sizeEstimate, k)

  /** Obtain a KmerTable from a single segment/superkmer */
  def fromSegment(segment: ZeroNTBitArray, k: Int, forwardOnly: Boolean, sort: Boolean = true): KmerTable =
    fromSegments(List(segment), Array(1), k, forwardOnly, sort)

  /**
   * Construct a KmerTable from super k-mers.
   *
   * @param segments    Super-mers
   * @param abundances  Abundances for each super-mer
   * @param k           k
   * @param forwardOnly Whether to filter out k-mers with reverse orientation
   * @param sort        Whether to sort the k-mers
   * @return
   */
  def fromSegments(segments: Iterable[NTBitArray], abundances: Array[Int], k: Int,
                   forwardOnly: Boolean, sort: Boolean = true): KmerTable = {
    val provider = new TagProvider {
      def tagWidth = 1
      override def writeForRowCol(row: Int, col: Int, to: KmerTableBuilder): Unit = {
        //Here, the abundance is the same for each column in the row
        to.addLong(abundances(row))
      }
    }
    fromSupermers(segments, k, forwardOnly, sort, provider)
  }

  /**
   * Write super-mers as k-mers, along with tag data, to a new KmerTable.
   *
   * @param supermers   Super k-mers
   * @param k           k
   * @param forwardOnly Whether to filter out k-mers with reverse orientation
   * @param sort        Whether to sort the result
   * @param tagData     Extra (tag) data for the given row and column, to be appended after the k-mer data
   * @return            The resulting KmerTable
   */
  def fromSupermers(supermers: Iterable[NTBitArray], k: Int, forwardOnly: Boolean,
                    sort: Boolean, tagData: TagProvider): KmerTable = {

    val estimatedSize = if (!forwardOnly) {
      //exact size can be known
      var sum = 0
      for { s <- supermers } sum += (s.size - (k - 1))
      sum
    } else {
      //generous estimate based on practical results for m=10,11
      supermers.size * 20
    }

    val n = KmerTable.longsForK(k)
    val tagWidth = tagData.tagWidth
    val builder = new KmerTableBuilder(n + tagWidth, tagWidth, estimatedSize, k)
    for { (s, row) <- supermers.iterator.zipWithIndex } {
      val provider = NestedRowTagProvider(row, tagData)
      s.writeKmersToBuilder(builder, k, forwardOnly, provider)
    }
    builder.result(sort)
  }
}

/** Builder for k-mer tables. K-mers are built by gradually adding longs in order.
 *
 * @param width        Width of k-mers (in longs, e.g. ceil(k/32)), including tag data
 * @param tagWidth     With of extra longs used to annotate k-mers with additional information (part of width)
 * @param sizeEstimate Estimated number of k-mers that will be inserted
 * @param k            k
 */
final class KmerTableBuilder(width: Int, tagWidth: Int, sizeEstimate: Int, k: Int) {
  private val builders = Array.fill(width)(new mutable.ArrayBuilder.ofLong)
  for (b <- builders) {
    b.sizeHint(sizeEstimate)
  }

  private var writeColumn = 0

  /** Add a single long value. Calling this method 'width' times adds a single k-mer to the table. */
  def addLong(x: Long): Unit = {
    builders(writeColumn) += x
    writeColumn += 1
    writeColumn = writeColumn % width
  }

  /** Add multiple long values. */
  def addLongs(xs: Array[Long]): Unit = {
    var i = 0
    while (i < xs.length) {
      addLong(xs(i))
      i += 1
    }
  }

  /** Construct a k-mer table that contains all the inserted k-mers.
   * After calling this method, this builder is invalid and should be discarded.
   *
   * @param sort Whether the k-mers should be sorted.
   * @return The resulting k-mer table
   */
  def result(sort: Boolean): KmerTable = {
    val r = builders.map(_.result())
    if (r(0).nonEmpty && sort) {
      LongArrays.radixSort(r)
    }
    width - tagWidth match {
      case 1 => new KmerTable1(r, width, tagWidth, k)
      case 2 => new KmerTable2(r, width, tagWidth, k)
      case 3 => new KmerTable3(r, width, tagWidth, k)
      case 4 => new KmerTable4(r, width, tagWidth, k)
      case _ => new KmerTableN(r, width, tagWidth, k)
    }
  }
}

/** A k-mer table is a collection of k-mers, stored in column-major format.
 * The first k-mer is stored in kmers(0)(0), kmers(1)(0), ... kmers(n)(0);
 * the second in kmers(0)(1), kmers(1)(1)... kmers(n)(1) and so on.
 * This layout enables fast radix sort.
 * The KmerTable is optionally sorted by construction (by KmerTableBuilder).
 * Each k-mer may contain additional annotation data ("tags") in longs following the sequence data itself.
 *
 * @param kmers k-mer data, column-major
 * @param width number of columns (longs per row) in the table, including k-mer and tag data
 * @param tagWidth number of additional columns on the right used for tag data
 * @param k length of k-mers
 */
abstract class KmerTable(val kmers: Array[Array[Long]], val width: Int, val tagWidth: Int, val k: Int)
  extends IndexedSeq[Array[Long]] {

  override val length = kmers(0).length

  /** K-mer only at position i */
  def apply(i: Int): Array[Long] =
    Array.tabulate(width - tagWidth)(x => kmers(x)(i))

  /** K-mer and tags at position i */
  def kmerWithTags(i: Int): Array[Long] =
    Array.tabulate(width)(x => kmers(x)(i))

  /** Tags only at position i */
  def tagsOnly(i: Int): Array[Long] =
    Array.tabulate(tagWidth)(x => kmers(x + width - tagWidth)(i))

  val kmerWidth: Int = width - tagWidth

  /** Test whether the k-mer at position i is equal to the given one. */
  def equalKmers(i: Int, kmer: Array[Long]): Boolean

  /**
   * Compare k-mer at position idx in this table with an equal length k-mer
   * at position otherIdx in the other table.
   * @param idx Index in this table
   * @param other Other table
   * @param otherIdx Index in other table
   * @return -1, 0, or 1 according to the Comparable contract (prior to, equal, or after)
   */
  def compareKmers(idx: Int, other: KmerTable, otherIdx: Int): Int

  /** Copy the k-mer at position i to a new long array. */
  def copyKmer(i: Int): Array[Long]

  /** Copy k-mer and tags at position i to a new long array. */
  def copyKmerAndTags(i: Int): Array[Long] =
    Array.tabulate(width)(j => kmers(j)(i))

  private def copyRangeToBuilder(destination: KmerTableBuilder, row: Int, from: Int, length: Int): Unit = {
    var x = from
    while (x < from + length) {
      destination.addLong(kmers(x)(row))
      x += 1
    }
  }

  /** Copy k-mer data only from position i to a builder. */
  def copyKmerOnlyToBuilder(destination: KmerTableBuilder, i: Int): Unit =
    copyRangeToBuilder(destination, i, 0, kmerWidth)

  /** Copy tag data only from position i to a builder. */
  def copyTagsOnlyToBuilder(destination: KmerTableBuilder, i: Int): Unit =
    copyRangeToBuilder(destination, i, kmerWidth, tagWidth)

  /** Copy k-mer and tag data from position i to a builder. */
  def copyKmerAndTagsToBuilder(destination: KmerTableBuilder, i: Int): Unit =
    copyRangeToBuilder(destination, i, 0, width)

  /** An iterator of distinct k-mers and their counts. Requires that the KmerTable was sorted at construction time. */
  def countedKmers: Iterator[(Array[Long], Abundance)] = new Iterator[(Array[Long], Abundance)] {
    private var i = 0
    private val len = KmerTable.this.size

    def hasNext: Boolean = i < len

    def next(): (Array[Long], Abundance) = {
      val lastKmer = copyKmer(i)
      var count: Abundance = kmers(kmerWidth)(i)
      if (!hasNext) {
        return (lastKmer, count)
      }
      i += 1
      while (i < len && equalKmers(i, lastKmer)) {
        count += kmers(kmerWidth)(i)
        i += 1
      }

      (lastKmer, count)
    }
  }

  def indexIterator: Iterator[Int] = Iterator.range(0, size)

  /** Iterator with k-mer data only */
  override def iterator: Iterator[Array[Long]] =
    indexIterator.map(i => Array.tabulate(width - tagWidth)(x => kmers(x)(i)))

  /** Iterator including both k-mer data and tag data */
  def iteratorWithTags: Iterator[Array[Long]] =
    indexIterator.map(i => copyKmerAndTags(i))

  /** Iterator including only tags data */
  def tagsIterator: Iterator[Array[Long]] =
    indexIterator.map(i => Array.tabulate(tagWidth)(x => kmers(kmerWidth + x)(i)))

  override def toString(): String = {
    val data = indices.map(i =>
      "[" + NTBitArray.longsToString(apply(i), 0, k) + "," + tagsOnly(i).toList.mkString(",") + "]")
    "KmerTable(\n" +
     "  " + data.mkString("\n  ") +
    ")"
    }
}

/**
 * Specialized KmerTable for n = 1 (k <= 32)
 * @param kmers k-mer data, column-major
 * @param width number of columns (longs per row) in the table, including k-mer and tag data
 * @param tagWidth number of additional columns on the right used for tag data
 * @param k length of k-mers
 */
final class KmerTable1(kmers: Array[Array[Long]], width: Int, tagWidth: Int, k: Int) extends
  KmerTable(kmers, width, tagWidth, k) {

  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    kmers(0)(i) == kmer(0)
  }

  def copyKmer(i: Int): Array[Long] = {
    Array(kmers(0)(i))
  }

  def compareKmers(idx: Int, other: KmerTable, otherIdx: Int): Int =
    java.lang.Long.compare(kmers(0)(idx), other.kmers(0)(otherIdx))

}

/**
 * Specialized KmerTable for n = 2 (k <= 64)
 * @param kmers k-mer data, column-major
 * @param width number of columns (longs per row) in the table, including k-mer and tag data
 * @param tagWidth number of additional columns on the right used for tag data
 * @param k length of k-mers
 */
final class KmerTable2(kmers: Array[Array[Long]], width: Int, tagWidth: Int, k: Int) extends KmerTable(kmers, width, tagWidth, k) {

  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    kmers(0)(i) == kmer(0) &&
      kmers(1)(i) == kmer(1)
  }

  def copyKmer(i: Int): Array[Long] = {
    Array(kmers(0)(i), kmers(1)(i))
  }

  def compareKmers(idx: Int, other: KmerTable, otherIdx: Int): Int = {
    import java.lang.Long.compare
    val r = compare(kmers(0)(idx), other.kmers(0)(otherIdx))
    if (r != 0) r else {
      compare(kmers(1)(idx), other.kmers(1)(otherIdx))
    }
  }
}

/**
 * Specialized KmerTable for n = 3 (k <= 96)
 * @param kmers k-mer data, column-major
 * @param width number of columns (longs per row) in the table, including k-mer and tag data
 * @param tagWidth number of additional columns on the right used for tag data
 * @param k length of k-mers
 */
final class KmerTable3(kmers: Array[Array[Long]], width: Int, tagWidth: Int, k: Int) extends KmerTable(kmers, width, tagWidth, k) {
  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    kmers(0)(i) == kmer(0) &&
      kmers(1)(i) == kmer(1) &&
      kmers(2)(i) == kmer(2)
  }

  def copyKmer(i: Int): Array[Long] = {
    Array(kmers(0)(i), kmers(1)(i), kmers(2)(i))
  }

  def compareKmers(idx: Int, other: KmerTable, otherIdx: Int): Int = {
    import java.lang.Long.compare
    var r = compare(kmers(0)(idx), other.kmers(0)(otherIdx))
    if (r != 0) return r
    r = compare(kmers(1)(idx), other.kmers(1)(otherIdx))
    if (r != 0) r else {
      compare(kmers(2)(idx), other.kmers(2)(otherIdx))
    }
  }
}

/**
 * Specialized KmerTable for n = 4 (k <= 128)
 * @param kmers k-mer data, column-major
 * @param width number of columns (longs per row) in the table, including k-mer and tag data
 * @param tagWidth number of additional columns on the right used for tag data
 * @param k length of k-mers
 */
final class KmerTable4(kmers: Array[Array[Long]], width: Int, tagWidth: Int, k: Int) extends KmerTable(kmers, width, tagWidth, k) {

  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    kmers(0)(i) == kmer(0) &&
      kmers(1)(i) == kmer(1) &&
      kmers(2)(i) == kmer(2) &&
      kmers(3)(i) == kmer(3)
  }

  def copyKmer(i: Int): Array[Long] = {
    Array(kmers(0)(i), kmers(1)(i), kmers(2)(i), kmers(3)(i))
  }

  def compareKmers(idx: Int, other: KmerTable, otherIdx: Int): Int = {
    import java.lang.Long.compare
    var r = compare(kmers(0)(idx), other.kmers(0)(otherIdx))
    if (r != 0) return r
    r = compare(kmers(1)(idx), other.kmers(1)(otherIdx))
    if (r != 0) return r
    r = compare(kmers(2)(idx), other.kmers(2)(otherIdx))
    if (r != 0) r else {
      compare(kmers(3)(idx), other.kmers(3)(otherIdx))
    }
  }
}

/**
 * General KmerTable for any value of n
 * @param kmers k-mer data, column-major
 * @param width number of columns (longs per row) in the table, including k-mer and tag data
 * @param tagWidth number of additional columns on the right used for tag data
 * @param k length of k-mers
 */
final class KmerTableN(kmers: Array[Array[Long]], width: Int, tagWidth: Int, k: Int)
  extends KmerTable(kmers, width, tagWidth, k) {

  def equalKmers(i: Int, kmer: Array[Long]): Boolean = {
    var j = 0
    while (j < kmerWidth) {
      if (kmers(j)(i) != kmer(j)) return false
      j += 1
    }
    true
  }

  def copyKmer(i: Int): Array[Long] =
    Array.tabulate(kmerWidth)(j => kmers(j)(i))

  def compareKmers(idx: Int, other: KmerTable, otherIdx: Int): Int = {
    import java.lang.Long.compare
    var j = 0
    while (j < kmerWidth - 1) {
      val r = compare(kmers(j)(idx), other.kmers(j)(otherIdx))
      if (r != 0) return r
      j += 1
    }
    compare(kmers(j)(idx), other.kmers(j)(otherIdx))
  }
}
